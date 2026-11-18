"""DataUpdateCoordinator for the Resideo integration — push via SignalR (spec §9).

Reads are **push-only**: there is no periodic poll (``update_interval=None``). REST is used only
to **bootstrap** at setup and to **resync** once on each (re)connect. Live state arrives over the
Azure SignalR stream and is merged into the cached shadow. If the stream fails, the coordinator
**reports an error** (entities go unavailable) — it never silently falls back to polling.
"""

from __future__ import annotations

import base64
from datetime import timedelta
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


def _decode_b64(raw_id: str | None) -> str:
    """Decode a base64 GraphQL ID to its trailing UUID/identifier."""
    if not raw_id:
        return ""
    try:
        decoded = base64.b64decode(raw_id + "=" * (-len(raw_id) % 4)).decode("utf-8", "ignore")
        if ":" in decoded:
            return decoded.split(":", 1)[1]
        return decoded
    except Exception:
        return raw_id


from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryError,
    ConfigEntryNotReady,
)
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .aioresideo import (
    LIVE_FEED_MERGED_PROPERTIES,
    Resideo,
    ResideoAccessory,
    ResideoAccountDevice,
    ResideoChangeConfirm,
    ResideoClient,
    ResideoConfiguration,
    ResideoLiveFeed,
    ResideoLocation,
    ResideoPriority,
    ResideoRooms,
    ResideoStream,
    ResideoThermostat,
    apply_live_feed,
)
from .aioresideo.exceptions import (
    ResideoApiError,
    ResideoAuthError,
    ResideoConnectionError,
    ResideoError,
    ResideoUnavailableError,
)
from .aioresideo.objects.events import clean_device_id
from .availability import (
    async_clear_unavailable,
    async_report_unavailable,
    unavailable_reason,
)
from .capture import ResideoCapture
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

type ResideoConfigEntry = ConfigEntry[ResideoDataUpdateCoordinator]

# Wait this long for the first SignalR connect + subscribe + activate during setup.
STREAM_CONNECT_TIMEOUT = 30

# Coalesce a burst of value-less stream events (settings ChangeRequests + unmerged LiveFeed types)
# into one REST resync, fired this many seconds after the first event (trailing debounce). Also lets
# a just-written value settle on the cloud before we re-read it.
RESYNC_DEBOUNCE = 3.0


@dataclass
class ResideoDeviceData:
    """Everything the coordinator holds for one thermostat."""

    thermostat: ResideoThermostat
    rooms: ResideoRooms  # /group/0/rooms — remote room sensors (may be empty)
    configuration: ResideoConfiguration  # /configuration — capabilities/equipment (may be empty)
    priority: ResideoPriority  # /priority — room priority/selection (may be empty)


@dataclass
class ResideoLeakDetectorData:
    """Everything the coordinator holds for one water leak & freeze detector."""

    mac: str
    name: str
    location_name: str | None = None
    location_id: str | None = None
    account_id: str | None = None
    consumer_device_id: str | None = None
    device_id_raw: str | None = None
    model: str | None = None
    product_name: str | None = None
    sku: str | None = None
    is_online: bool = True
    water_present: bool = False
    temperature: float | None = None
    humidity: float | None = None
    battery_level: int | None = None
    last_updated: float | None = None


class ResideoDataUpdateCoordinator(DataUpdateCoordinator[dict[str, ResideoDeviceData]]):
    """Holds the shadow + room sensors of every thermostat, fed by the SignalR live stream."""

    config_entry: ResideoConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ResideoConfigEntry,
        api: Resideo,
        capture: ResideoCapture,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=None,  # push-only — no periodic poll
        )
        self.api = api
        # What the cloud pushed and how it answered our writes, for the diagnostics download.
        self.capture = capture
        self.probe_results: dict[str, Any] = {}
        self._macs: list[str] = []
        self._leak_macs: list[str] = []
        self.leak_detectors: dict[str, ResideoLeakDetectorData] = {}
        self._targets: list[ResideoLocation] = []
        # The account's country, read once at setup. It decides the unit of the outdoor
        # temperature, the one reading that isn't always °F (see ``outdoor_temperature_unit``).
        self.country_code: str | None = None
        self._streams: list[ResideoStream] = []
        # Settings (Feels Like, Adaptive Recovery, ...) and unmerged value-types carry no usable
        # value on the stream; a stream event schedules this debounced REST resync to fetch them.
        self._resync_debouncer = Debouncer(
            hass,
            _LOGGER,
            cooldown=RESYNC_DEBOUNCE,
            immediate=False,
            function=self.async_refresh,
        )
        # accessoryValue (remote-sensor occupancy config) has no per-field merge, so every write
        # must send the full body — but its three controls (sensitivity + the two exclude flags)
        # are separate entities. These per-accessory optimistic overrides are the shared source for
        # composing that body: without them a write for one field would echo the others from the
        # shadow, which lags a ``202`` by ~2 s, so a second write inside that window would clobber
        # the first. Keyed by (mac, accessory_id); each field is cleared once a refresh confirms it.
        self._accessory_overrides: dict[tuple[str, int], dict[str, Any]] = {}
        # setPointCapabilities has the same no-merge gotcha: a partial body 202s but is silently
        # ignored, so every write must carry all four limits. Same shared-override trick (keyed by
        # mac) so two limit edits inside the ~2 s read-back window can't clobber each other.
        self._setpoint_limit_overrides: dict[str, dict[str, Any]] = {}

    async def _async_setup(self) -> None:
        """One-time discovery from the account graph: thermostats, leak detectors, SignalR targets, country."""
        try:
            accounts = await self.api.async_get_accounts()
        except ResideoAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except ResideoUnavailableError as err:
            async_report_unavailable(self.hass, self.config_entry, err)
            raise UpdateFailed(unavailable_reason(err)) from err
        except (ResideoConnectionError, ResideoError) as err:
            raise UpdateFailed(str(err)) from err
        devices = [ResideoAccountDevice(d) for d in ResideoClient.iter_devices(accounts)]
        self._targets = ResideoClient.iter_locations(accounts)
        self.country_code = ResideoClient.country_code(accounts)
        self._macs = [d.mac for d in devices if d.is_thermostat and d.mac]
        self._leak_macs = [d.mac for d in devices if d.is_leak_detector and d.mac]

        for d in devices:
            if d.is_leak_detector and d.mac:
                prod = d.attributes.get("product") or {}
                self.leak_detectors[d.mac] = ResideoLeakDetectorData(
                    mac=d.mac,
                    name=d.name or f"Leak Detector {d.mac}",
                    location_name=d.location,
                    location_id=d.attributes.get("locationId"),
                    account_id=d.attributes.get("accountId"),
                    consumer_device_id=d.attributes.get("consumerDeviceId"),
                    device_id_raw=d.attributes.get("id") or d.attributes.get("globalId"),
                    model=d.model or "LeakDetector_L1_R",
                    product_name=prod.get("productName") or "L1 WiFi Water Leak & Freeze Detector",
                    sku=d.attributes.get("sku"),
                    is_online=True,
                    water_present=False,
                )

        if not self._macs and not self._leak_macs:
            # A stable account-level condition (e.g. only Lyric/LCC-platform devices, which
            # live on a system this API can't reach) -> fail setup rather than retry.
            if devices:
                found = ", ".join(
                    f"{d.model or 'unknown model'} ({d.device_kind or 'unknown kind'})"
                    for d in devices
                )
                raise ConfigEntryError(
                    f"No supported devices in this Resideo account — found: {found}. "
                    "This integration reads devices on the platform the Resideo Connect app "
                    "(formerly First Alert) uses; a device that only appears in the older "
                    "Resideo - Smart Home app (formerly Honeywell Home) is on a separate Resideo "
                    "system it cannot reach."
                )
            raise ConfigEntryError(
                "No devices found in this Resideo account. If your device only appears in the "
                "older Resideo - Smart Home app (formerly Honeywell Home), it is on a separate "
                "Resideo system this integration cannot reach."
            )
        _LOGGER.debug(
            "Discovered %d thermostat(s) %s and %d leak detector(s) %s across %d location(s), account country %s",
            len(self._macs),
            self._macs,
            len(self._leak_macs),
            self._leak_macs,
            len(self._targets),
            self.country_code,
        )
        if self._leak_macs:
            # Water leak detectors are battery-powered and do not send real-time
            # SignalR LiveFeedEvents for routine telemetry. Poll Resideo cloud
            # periodically so newly checked-in sensor data reaches Home Assistant.
            self.update_interval = timedelta(minutes=15)

        async_clear_unavailable(self.hass, self.config_entry)


    async def _async_update_data(self) -> dict[str, ResideoDeviceData]:
        """Bootstrap / resync read (NOT a periodic poll).

        Reads the shadow + rooms + configuration + priority per thermostat. The ``/priority`` read
        also (re)warms the SignalR live feed (spec §9.2). Runs at setup and once per (re)connect.

        Failures are isolated per device: a failing thermostat keeps its previous snapshot (or
        drops out if it never had one) so the others stay live; only every-device-failing raises.
        """
        result: dict[str, ResideoDeviceData] = {}
        failures: list[str] = []
        # Kept as the exception, not just its text: a total 503 is reported to the user very
        # differently from an ordinary read failure (see availability.py).
        unavailable: ResideoUnavailableError | None = None
        for mac in self._macs:
            try:
                result[mac] = await self.async_read_device(mac)
                self.capture.record_read(mac)
            except ResideoAuthError as err:
                raise ConfigEntryAuthFailed(str(err)) from err
            except (ResideoConnectionError, ResideoError) as err:
                if isinstance(err, ResideoUnavailableError):
                    unavailable = err
                failures.append(f"{mac}: {err}")
                previous = (self.data or {}).get(mac)
                if previous is not None:
                    result[mac] = previous
        if failures:
            if len(failures) == len(self._macs):
                if unavailable is not None:
                    async_report_unavailable(self.hass, self.config_entry, unavailable)
                    raise UpdateFailed(unavailable_reason(unavailable))
                raise UpdateFailed("; ".join(failures))
            _LOGGER.warning(
                "Resync failed for %d of %d thermostat(s): %s",
                len(failures),
                len(self._macs),
                "; ".join(failures),
            )
        if self._leak_macs:
            await self._async_refresh_leak_detectors()
        # Something got through, so the cloud is answering us again.
        async_clear_unavailable(self.hass, self.config_entry)
        return result

    async def _async_refresh_leak_detectors(self) -> None:
        """Best-effort initial/resync refresh of leak detectors from location devices and telemetry endpoints."""
        queried_locations: set[str] = set()
        for leak in self.leak_detectors.values():
            acc_raw = leak.account_id or ""
            acc_uuid = _decode_b64(acc_raw)
            loc_raw = leak.location_id or ""
            loc_uuid = _decode_b64(loc_raw)

            # 1. Location-level device collections & status
            loc_candidates = [
                # Base collection (confirmed 200)
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/devices",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/devices",

                # Location consumerDevices & waterLeakDetectors collections
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices",
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/waterLeakDetectors",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/waterLeakDetectors",
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/waterLeakDetector",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/waterLeakDetector",
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/leakDetectors",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/leakDetectors",

                # Non-ris paths
                f"/v2/locations/{loc_raw}/devices",
                f"/v1/locations/{loc_raw}/devices",
                f"/v2/locations/{loc_uuid}/devices",
                f"/v1/locations/{loc_uuid}/devices",
                f"/v2/locations",
                f"/v1/locations",
                f"/v2/devices?locationId={loc_raw}",
                f"/v2/devices/waterLeakDetectors?locationId={loc_raw}",
                f"/v2/devices/waterLeakDetector?locationId={loc_raw}",
            ]
            for loc_ep in loc_candidates:
                if not loc_raw or loc_ep in queried_locations:
                    continue
                queried_locations.add(loc_ep)
                try:
                    resp = await self.api.client.get(loc_ep)
                    self.probe_results[loc_ep] = {"status": 200, "data": resp}
                    _LOGGER.warning("PROBE LOCATION SUCCESS [200]: %s -> %s", loc_ep, str(resp)[:200])
                    for l_mac in self.leak_detectors:
                        self._update_leak_detector_from_dict(l_mac, resp)
                except ResideoApiError as err:
                    self.probe_results[loc_ep] = {"status": err.status, "body": str(err.body)[:100]}
                    _LOGGER.debug("PROBE LOCATION [%s]: %s", err.status, loc_ep)
                except Exception as err:
                    self.probe_results[loc_ep] = {"error": str(err)}
                    _LOGGER.debug("PROBE LOCATION [ERR]: %s -> %s", loc_ep, err)

        # 2. Per-device candidate telemetry endpoints
        for mac, leak in self.leak_detectors.items():
            acc_raw = leak.account_id or ""
            acc_uuid = _decode_b64(acc_raw)
            loc_raw = leak.location_id or ""
            loc_uuid = _decode_b64(loc_raw)
            cd_raw = leak.consumer_device_id or ""
            cd_uuid = _decode_b64(cd_raw)
            dev_raw = leak.device_id_raw or ""

            candidates = [
                # 1. waterLeakDetectors (plural, the standard Resideo collection route)
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{mac}",
                f"/ris-public-api/api/v1/devices/waterLeakDetectors/{mac}",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v1/devices/waterLeakDetectors/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v1/devices/waterLeakDetectors/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{mac}/status",
                f"/ris-public-api/api/v1/devices/waterLeakDetectors/{mac}/status",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{mac}/state",
                f"/ris-public-api/api/v1/devices/waterLeakDetectors/{mac}/state",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{cd_raw}",
                f"/ris-public-api/api/v2/devices/waterLeakDetectors/{cd_uuid}",

                # 2. waterLeakDetector (singular)
                f"/ris-public-api/api/v2/devices/waterLeakDetector/{mac}",
                f"/ris-public-api/api/v1/devices/waterLeakDetector/{mac}",
                f"/ris-public-api/api/v2/devices/waterLeakDetector/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v2/devices/waterLeakDetector/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v1/devices/waterLeakDetector/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v1/devices/waterLeakDetector/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v2/devices/waterLeakDetector/{cd_raw}",
                f"/ris-public-api/api/v2/devices/waterLeakDetector/{cd_uuid}",

                # 3. leakDetectors and leakDetector
                f"/ris-public-api/api/v2/devices/leakDetectors/{mac}",
                f"/ris-public-api/api/v1/devices/leakDetectors/{mac}",
                f"/ris-public-api/api/v2/devices/leakDetectors/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v2/devices/leakDetectors/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v2/devices/leakDetector/{mac}",
                f"/ris-public-api/api/v1/devices/leakDetector/{mac}",
                f"/ris-public-api/api/v2/devices/leakDetector/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v2/devices/leakDetector/{mac}?locationId={loc_uuid}",

                # 4. consumerDevices direct routes
                f"/ris-public-api/api/v2/consumerDevices/{cd_raw}",
                f"/ris-public-api/api/v1/consumerDevices/{cd_raw}",
                f"/ris-public-api/api/v2/consumerDevices/{cd_uuid}",
                f"/ris-public-api/api/v1/consumerDevices/{cd_uuid}",
                f"/ris-public-api/api/v2/consumerDevices/{mac}",
                f"/ris-public-api/api/v1/consumerDevices/{mac}",
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices/{cd_raw}",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices/{cd_raw}",
                f"/ris-public-api/api/v2/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices/{mac}",
                f"/ris-public-api/api/v1/accounts/{acc_raw}/locations/{loc_raw}/consumerDevices/{mac}",

                # 5. Generic /devices/{mac} without device type
                f"/ris-public-api/api/v2/devices/{mac}",
                f"/ris-public-api/api/v1/devices/{mac}",
                f"/ris-public-api/api/v2/devices/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v2/devices/{mac}?locationId={loc_uuid}",
                f"/ris-public-api/api/v1/devices/{mac}?locationId={loc_raw}",
                f"/ris-public-api/api/v1/devices/{mac}?locationId={loc_uuid}",

                # 6. Non-ris paths
                f"/v2/devices/waterLeakDetectors/{mac}",
                f"/v1/devices/waterLeakDetectors/{mac}",
                f"/v2/devices/waterLeakDetectors/{mac}?locationId={loc_raw}",
                f"/v2/devices/waterLeakDetectors/{mac}?locationId={loc_uuid}",
                f"/v2/devices/waterLeakDetector/{mac}",
                f"/v1/devices/waterLeakDetector/{mac}",
                f"/v2/devices/waterLeakDetector/{mac}?locationId={loc_raw}",
                f"/v2/devices/{mac}",
                f"/v1/devices/{mac}",
                f"/v2/devices/{mac}?locationId={loc_raw}",
                f"/v2/devices/{mac}?locationId={loc_uuid}",

                # 7. devsrv
                f"/devsrv/api/v2/device/{mac}",
                f"/devsrv/api/v1/device/{mac}",
                f"/devsrv/api/v2/devices/{mac}",
            ]
            for endpoint in candidates:
                if not endpoint:
                    continue
                try:
                    dev_data = await self.api.client.get(endpoint)
                    self.probe_results[endpoint] = {"status": 200, "data": dev_data}
                    _LOGGER.warning("PROBE SUCCESS [200]: %s -> %s", endpoint, str(dev_data)[:300])
                    self._update_leak_detector_from_dict(mac, dev_data)
                except ResideoApiError as err:
                    self.probe_results[endpoint] = {"status": err.status, "body": str(err.body)[:100]}
                    _LOGGER.debug("PROBE [%s]: %s", err.status, endpoint)
                except Exception as err:
                    self.probe_results[endpoint] = {"error": str(err)}
                    _LOGGER.debug("PROBE [ERR]: %s -> %s", endpoint, err)

            # 8. GraphQL probe
            for gql_ep in [
                "/ris-public-api/api/graphql",
                "/ris-public-api/graphql",
                "/graphql",
                "/api/graphql",
            ]:
                if gql_ep in queried_locations:
                    continue
                queried_locations.add(gql_ep)
                try:
                    gql_body = {
                        "query": f'query {{ node(id: "{cd_raw}") {{ id ... on ConsumerDevice {{ id name }} }} }}'
                    }
                    gql_data = await self.api.client.post(gql_ep, json=gql_body)
                    self.probe_results[f"POST {gql_ep}"] = {"status": 200, "data": gql_data}
                    _LOGGER.warning("PROBE GQL SUCCESS [200]: %s -> %s", gql_ep, str(gql_data)[:300])
                    self._update_leak_detector_from_dict(mac, gql_data)
                except ResideoApiError as err:
                    self.probe_results[f"POST {gql_ep}"] = {"status": err.status, "body": str(err.body)[:100]}
                except Exception as err:
                    self.probe_results[f"POST {gql_ep}"] = {"error": str(err)}

    def _update_leak_detector_from_dict(self, mac: str, data: Any) -> None:
        if mac not in self.leak_detectors:
            return
        leak = self.leak_detectors[mac]
        changed = False

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    item_dev_id = self._find_first(item, "deviceId", "mac")
                    if item_dev_id and clean_device_id(item_dev_id) == mac:
                        self._update_leak_detector_from_dict(mac, item)
                        return
                    if leak.consumer_device_id and item.get("id") == leak.consumer_device_id:
                        self._update_leak_detector_from_dict(mac, item)
                        return
                    if leak.device_id_raw and item.get("device", {}).get("id") == leak.device_id_raw:
                        self._update_leak_detector_from_dict(mac, item)
                        return
            if len(data) == 1 and isinstance(data[0], dict):
                self._update_leak_detector_from_dict(mac, data[0])
            return

        if not isinstance(data, dict):
            return

        if "data" in data and isinstance(data["data"], (dict, list)):
            self._update_leak_detector_from_dict(mac, data["data"])
            return

        # If this dict explicitly declares a different device, don't parse it for this MAC
        direct_dev_id = self._find_first(data, "deviceId")
        if direct_dev_id and clean_device_id(direct_dev_id) != mac:
            return

        # 1. Moisture / Water Present
        water = self._find_first(
            data,
            "waterPresent",
            "water_present",
            "leakDetected",
            "water",
            "leak",
            "isWaterDetected",
            "waterDetection",
            "moisture",
        )
        if water is not None:
            old_water = leak.water_present
            if isinstance(water, bool):
                leak.water_present = water
            elif isinstance(water, (int, float)):
                leak.water_present = bool(water)
            elif isinstance(water, str):
                leak.water_present = water.lower() in ("true", "1", "leak", "water", "wet", "alarm")
            elif isinstance(water, dict):
                st = str(water.get("deviceState", "")).lower()
                leak.water_present = st in ("alarm", "water", "leak", "wet")
            if leak.water_present != old_water:
                changed = True

        alarms = self._find_first(data, "currentAlarms", "alarms", "alarmState", "thermostatFaults")
        if isinstance(alarms, list):
            for alarm in alarms:
                alarm_str = str(alarm).lower()
                if "leak" in alarm_str or "water" in alarm_str:
                    if not leak.water_present:
                        leak.water_present = True
                        changed = True
        elif isinstance(alarms, dict):
            w_alarm = alarms.get("water") or alarms.get("leak")
            if isinstance(w_alarm, dict) and str(w_alarm.get("deviceState", "")).lower() == "alarm":
                if not leak.water_present:
                    leak.water_present = True
                    changed = True

        # 2. Temperature
        temp = self._find_first(
            data,
            "displayedIndoorTemperature",
            "indoorTemperature",
            "temperature",
            "temp",
            "indoor_temperature",
            "currentTemperature",
            "tempF",
        )
        if temp is not None:
            try:
                val_temp = float(temp)
                units = str(self._find_first(data, "units", "tempUnit", "temperatureUnit") or "").lower()
                if units in ("celsius", "c") or (units == "" and val_temp < 45.0):
                    val_temp = round(val_temp * 9.0 / 5.0 + 32.0, 1)
                else:
                    val_temp = round(val_temp, 1)
                if leak.temperature != val_temp:
                    leak.temperature = val_temp
                    changed = True
            except (ValueError, TypeError):
                pass

        # 3. Humidity
        hum = self._find_first(
            data,
            "displayedIndoorHumidity",
            "indoorHumidity",
            "humidity",
            "hum",
            "humid",
            "indoor_humidity",
            "currentHumidity",
        )
        if hum is not None:
            try:
                val_hum = round(float(hum), 1)
                if leak.humidity != val_hum:
                    leak.humidity = val_hum
                    changed = True
            except (ValueError, TypeError):
                pass

        # 4. Battery
        batt = self._find_first(
            data,
            "batteryRemaining",
            "batteryLevel",
            "battery",
            "battery_level",
            "batteryPercentage",
            "batteryStatus",
            "batteryState",
        )
        if batt is not None:
            try:
                if isinstance(batt, dict):
                    batt = batt.get("deviceState") or batt.get("level") or batt.get("remaining")
                if isinstance(batt, (int, float)):
                    val_batt = int(batt)
                elif isinstance(batt, str):
                    if batt.isdigit():
                        val_batt = int(batt)
                    elif batt.lower() in ("good", "normal", "ok", "high"):
                        val_batt = 100
                    elif batt.lower() in ("low", "warning"):
                        val_batt = 20
                    elif batt.lower() in ("critical", "empty"):
                        val_batt = 5
                    else:
                        val_batt = None
                else:
                    val_batt = None
                if val_batt is not None and leak.battery_level != val_batt:
                    leak.battery_level = val_batt
                    changed = True
            except (ValueError, TypeError):
                pass

        # 5. Connectivity
        online = self._find_first(data, "isOnline", "online", "connected", "connectionStatus")
        if online is not None:
            if isinstance(online, bool):
                val_online = online
            elif isinstance(online, str):
                val_online = online.lower() in ("true", "1", "online", "connected")
            else:
                val_online = bool(online)
            if leak.is_online != val_online:
                leak.is_online = val_online
                changed = True

        if changed:
            _LOGGER.info(
                "Updated leak detector %s: temp=%s, humidity=%s, battery=%s, water=%s, online=%s",
                mac,
                leak.temperature,
                leak.humidity,
                leak.battery_level,
                leak.water_present,
                leak.is_online,
            )
            self.async_update_listeners()

    @staticmethod
    def _find_first(d: Any, *keys: str) -> Any:
        """Deeply search a dict or list for the first occurrence of any matching key (case-insensitive)."""
        lower_keys = {k.lower() for k in keys}
        if isinstance(d, dict):
            for k, v in d.items():
                if str(k).lower() in lower_keys and v is not None:
                    return v
            for v in d.values():
                if isinstance(v, (dict, list)):
                    res = ResideoDataUpdateCoordinator._find_first(v, *keys)
                    if res is not None:
                        return res
        elif isinstance(d, list):
            for item in d:
                res = ResideoDataUpdateCoordinator._find_first(item, *keys)
                if res is not None:
                    return res
        return None


    async def async_read_device(self, mac: str) -> ResideoDeviceData:
        """Read one thermostat's full snapshot.

        Rooms / configuration / priority are best-effort: a stripped-down thermostat may 404
        on these, but the shadow must succeed.

        A 503 is the exception to "best-effort". ``ResideoUnavailableError`` is a
        ``ResideoApiError``, so without re-raising it first these arms would quietly turn "Resideo
        is refusing us" into "this thermostat has no room sensors" — and since the reads are split
        across API versions (``/priority`` and ``/group/0/rooms`` are v1, the shadow is v2 — see
        ``aioresideo/const.py``), a version-scoped refusal would strip every room sensor off the
        device while the shadow kept working and nothing ever reported a problem.
        """
        thermostat = await self.api.async_get_device(mac)
        try:
            rooms = await self.api.async_get_rooms(mac)
        except ResideoUnavailableError:
            raise
        except ResideoApiError as err:
            _LOGGER.debug("No room sensors for %s (%s)", mac, err)
            rooms = ResideoRooms({})
        try:
            configuration = await self.api.async_get_configuration(mac)
        except ResideoUnavailableError:
            raise
        except ResideoApiError as err:
            _LOGGER.debug("No configuration for %s (%s)", mac, err)
            configuration = ResideoConfiguration({})
        try:
            priority = await self.api.async_get_priority(mac)
        except ResideoUnavailableError:
            raise
        except ResideoApiError as err:
            _LOGGER.debug("No priority for %s (%s)", mac, err)
            priority = ResideoPriority({})
        return ResideoDeviceData(
            thermostat=thermostat,
            rooms=rooms,
            configuration=configuration,
            priority=priority,
        )

    # -- full-body writes (optimism-coalesced; see ``__init__``) ---------------
    @staticmethod
    async def _async_write_with_override(
        override: dict[str, Any],
        field: str,
        value: Any,
        write: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        """Record ``override[field] = value`` and send the full body via ``write(override)``.

        On failure the override is reverted, so a failed write can't poison later full-body
        composes. The calling entity clears its field once a refresh confirms it.
        """
        had_previous = field in override
        previous = override.get(field)
        override[field] = value
        try:
            return await write(override)
        except Exception:
            if had_previous:
                override[field] = previous
            else:
                override.pop(field, None)
            raise

    def accessory_override(self, mac: str, accessory_id: int) -> dict[str, Any]:
        """Shared optimistic overrides for one accessory's writable fields (see ``__init__``)."""
        return self._accessory_overrides.setdefault((mac, accessory_id), {})

    async def async_write_accessory_value(
        self,
        mac: str,
        accessory_id: int,
        accessory: ResideoAccessory,
        *,
        field: str,
        value: Any,
    ) -> dict[str, Any]:
        """Set one ``accessoryValue`` field, sending the full body from the shared overrides.

        ``field`` is one of ``sensitivity`` / ``exclude_motion`` / ``exclude_temp`` (the API kwargs).
        Fields with no pending override fall back to the current shadow ``accessory`` snapshot.
        """
        return await self._async_write_with_override(
            self.accessory_override(mac, accessory_id),
            field,
            value,
            lambda ov: self.api.async_set_accessory_value(
                mac,
                accessory_id,
                sensitivity=ov.get("sensitivity", accessory.occupancy_sensitivity),
                exclude_motion=ov.get("exclude_motion", accessory.exclude_motion),
                exclude_temp=ov.get("exclude_temp", accessory.exclude_temperature),
            ),
        )

    def setpoint_limit_override(self, mac: str) -> dict[str, Any]:
        """Shared optimistic overrides for a device's four setpoint limits (see ``__init__``)."""
        return self._setpoint_limit_overrides.setdefault(mac, {})

    async def async_write_setpoint_limit(
        self,
        mac: str,
        configuration: ResideoConfiguration,
        *,
        field: str,
        value: float,
    ) -> dict[str, Any]:
        """Set one setpoint limit, sending the full four-field body from the shared overrides.

        ``field`` is one of ``heat_min`` / ``heat_max`` / ``cool_min`` / ``cool_max``. Fields with
        no pending override fall back to the current ``configuration`` snapshot.
        """
        return await self._async_write_with_override(
            self.setpoint_limit_override(mac),
            field,
            value,
            lambda ov: self.api.async_set_setpoint_capabilities(
                mac,
                heat_min=ov.get("heat_min", configuration.min_heat_setpoint),
                heat_max=ov.get("heat_max", configuration.max_heat_setpoint),
                cool_min=ov.get("cool_min", configuration.min_cool_setpoint),
                cool_max=ov.get("cool_max", configuration.max_cool_setpoint),
            ),
        )

    # -- SignalR streaming ----------------------------------------------------
    async def async_start_streams(self) -> None:
        """Build + connect one stream per location with a thermostat. Raises on first-connect failure.

        On success, schedules each supervisor (``async_run``) as a background task. A connect
        failure raises ``ConfigEntryNotReady`` / ``ConfigEntryAuthFailed`` so setup is retried —
        push is the contract, so we never proceed without it.
        """
        known = set(self._macs) | set(self._leak_macs)
        for target in self._targets:
            device_ids = [m for m in target.device_ids if m in known]
            if not device_ids or not target.node_id:
                continue
            self._streams.append(
                self.api.create_stream(
                    target.node_id,
                    device_ids,
                    self._on_stream_event,
                    on_connected=self._on_stream_connected,
                    on_error=self._on_stream_error,
                )
            )
        total_devices = len(self._macs) + len(self._leak_macs)
        if not self._streams:
            raise ConfigEntryNotReady(
                f"Found {total_devices} device(s), but no account location "
                "supports live updates (SignalR)"
            )

        try:
            for stream in self._streams:
                await stream.async_connect_once_or_raise(STREAM_CONNECT_TIMEOUT)
        except ResideoAuthError as err:
            await self.async_stop_streams()
            raise ConfigEntryAuthFailed(str(err)) from err
        except (ResideoError, OSError, TimeoutError) as err:
            await self.async_stop_streams()
            raise ConfigEntryNotReady(f"SignalR stream failed to start: {err}") from err

        for stream in self._streams:
            self.config_entry.async_create_background_task(
                self.hass, stream.async_run(), name=f"{DOMAIN}_signalr"
            )

    async def async_stop_streams(self) -> None:
        """Tear down all streams (graceful unsubscribe + close) + cancel the resync debouncer."""
        streams, self._streams = self._streams, []
        for stream in streams:
            await stream.async_stop()
        self._resync_debouncer.async_shutdown()

    @callback
    def _on_stream_event(self, event: ResideoLiveFeed | ResideoChangeConfirm) -> None:
        """Apply one SignalR event to the cached data (called on the loop from the recv loop)."""
        if isinstance(event, ResideoChangeConfirm):
            self.capture.record_change(event)
            if not event.success:
                # The cloud accepted (202) but the device/service rejected the change — the
                # optimistic UI value will be rolled back by the reconcile refresh.
                _LOGGER.warning(
                    "Resideo rejected change %s (txn=%s) for %s",
                    event.change_name,
                    event.transaction_id,
                    event.device_id,
                )
                return
            _LOGGER.debug(
                "ChangeRequest %s ok txn=%s", event.change_name, event.transaction_id
            )
            # Settings changes (Feels Like, Adaptive Recovery, schedule, reminders, ...) ride
            # ChangeRequest and carry NO values -> re-read the shadow to reflect them (spec §9a/§9.1).
            self.schedule_resync()
            return
        if not isinstance(event, ResideoLiveFeed):
            return
        # The raw push is the evidence in most bug reports (the cloud's shapes are reverse-
        # engineered — e.g. every Sensor push is labelled with the thermostat's ids), so it is
        # both sampled for diagnostics and logged, merged or not.
        self.capture.record_push(event)

        # Handle leak detector stream events
        mac = event.device_id
        if mac and mac in self.leak_detectors:
            self._handle_leak_stream_event(event)
            return

        if not self.data:
            return
        _LOGGER.debug("LiveFeed %s %s: %s", event.device_id, event.property_name, event.value)
        current = self.data.get(event.device_id)
        if current is None:
            return  # event for a device we don't track
        if event.property_name not in LIVE_FEED_MERGED_PROPERTIES:
            # A value-bearing type we don't merge in-memory (Schedule*, DrEventStatus, ...) ->
            # resync to reflect it without guessing its push shape.
            self.schedule_resync()
            return
        new_shadow, new_rooms = apply_live_feed(
            current.thermostat.attributes, current.rooms.attributes, event
        )
        updated = ResideoDeviceData(
            thermostat=ResideoThermostat(new_shadow),
            rooms=ResideoRooms(new_rooms),
            configuration=current.configuration,
            priority=current.priority,
        )
        self.capture.record_merge(event.device_id)
        # Pushes the new data AND clears any prior error state -> entities available again.
        self.async_set_updated_data({**self.data, event.device_id: updated})

    @callback
    def _handle_leak_stream_event(self, event: ResideoLiveFeed) -> None:
        """Handle real-time push event for a water leak detector."""
        mac = event.device_id
        if not mac or mac not in self.leak_detectors:
            return
        leak = self.leak_detectors[mac]
        prop = (event.property_name or "").lower()
        val = event.value

        _LOGGER.info("LeakDetector LiveFeed received for %s (%s): %s", mac, event.property_name, val)

        if isinstance(val, dict):
            self._update_leak_detector_from_dict(mac, val)
        else:
            changed = False
            if "water" in prop or "leak" in prop:
                old_w = leak.water_present
                if isinstance(val, bool):
                    leak.water_present = val
                elif isinstance(val, (int, float)):
                    leak.water_present = bool(val)
                elif isinstance(val, str):
                    leak.water_present = val.lower() in ("true", "1", "leak", "water", "wet", "alarm")
                if leak.water_present != old_w:
                    changed = True
            elif "temp" in prop:
                try:
                    val_t = float(val)
                    if leak.temperature != val_t:
                        leak.temperature = val_t
                        changed = True
                except (ValueError, TypeError):
                    pass
            elif "humid" in prop:
                try:
                    val_h = float(val)
                    if leak.humidity != val_h:
                        leak.humidity = val_h
                        changed = True
                except (ValueError, TypeError):
                    pass
            elif "batt" in prop:
                try:
                    val_b = int(val)
                    if leak.battery_level != val_b:
                        leak.battery_level = val_b
                        changed = True
                except (ValueError, TypeError):
                    pass
            elif "online" in prop or "connect" in prop:
                old_on = leak.is_online
                leak.is_online = bool(val)
                if leak.is_online != old_on:
                    changed = True

            if changed:
                self.async_update_listeners()



    @callback
    def schedule_resync(self) -> None:
        """Schedule the debounced REST resync (coalesces bursts into one refresh).

        Used for value-less stream events (settings ChangeRequests, unmerged LiveFeed types)
        and by entities after a write — the optimistic value covers the UI meanwhile, and the
        debounce lets the just-written value settle on the cloud before it is re-read.
        """
        self._resync_debouncer.async_schedule_call()

    async def _on_stream_connected(self) -> None:
        """Resync once on each (re)connect — catch changes missed while down + never-pushed fields."""
        await self.async_refresh()

    @callback
    def _on_stream_error(self, err: Exception) -> None:
        """A sustained stream failure -> entities unavailable (reported, not masked by polling)."""
        if isinstance(err, ResideoUnavailableError):
            async_report_unavailable(self.hass, self.config_entry, err)
            self.async_set_update_error(UpdateFailed(unavailable_reason(err)))
            return
        self.async_set_update_error(UpdateFailed(str(err)))
        if isinstance(err, ResideoAuthError):
            self.config_entry.async_start_reauth(self.hass)
