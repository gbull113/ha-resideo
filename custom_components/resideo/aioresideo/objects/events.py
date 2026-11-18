"""Event models for the SignalR live stream (see ``resideo-api-spec.md`` §9).

The data-sync hub invokes the client method ``events`` with one JSON string per notification.
Two families behave very differently:
  - ``LiveFeedEvent``       — real-time device state **with values** (``Body.PropertyName`` /
                              ``Body.Value`` / ``Body.SubscriptionExpiration``).
  - ``ChangeRequestSuccess`` / ``ChangeRequestFailure`` — command-dispatch confirmations
                              (``Body.TransactionId`` matches a REST write's ``202``); no values.

:func:`parse_event` turns a raw frame argument (a JSON string or already-decoded dict) into a
typed event, or ``None`` for input we don't model.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResideoLiveFeed:
    """A ``LiveFeedEvent`` — pushed device state with values (spec §9b)."""

    device_id: str | None
    property_name: str | None
    value: Any
    subscription_expiration: str | None = None


@dataclass(frozen=True)
class ResideoChangeConfirm:
    """A ``ChangeRequestSuccess`` / ``ChangeRequestFailure`` — a command dispatched (spec §9a)."""

    device_id: str | None
    transaction_id: str | None
    change_name: str | None
    change_direction: str | None
    success: bool


ResideoEvent = ResideoLiveFeed | ResideoChangeConfirm


import base64


def clean_device_id(raw_id: Any) -> str | None:
    """Normalize a device ID (MAC or GraphQL global id) to uppercase 12-char hex MAC."""
    if not raw_id:
        return None
    val = str(raw_id).strip()
    if "=" in val or len(val) > 20:
        try:
            decoded = base64.b64decode(val + "=" * (-len(val) % 4)).decode("utf-8", "ignore")
            if ":" in decoded:
                val = decoded.split(":")[-1]
        except Exception:
            pass
    if ":" in val and val.count(":") != 5:
        val = val.split(":")[-1]
    return val.replace(":", "").replace("-", "").upper()


def parse_event(arg: str | dict[str, Any]) -> ResideoEvent | None:
    """Parse one ``events`` argument into a typed event (``None`` if unparseable/unmodeled)."""
    if isinstance(arg, str):
        try:
            notif = json.loads(arg)
        except (ValueError, TypeError):
            return None
    else:
        notif = arg
    if not isinstance(notif, dict):
        return None

    notif_type = notif.get("NotificationType")
    body = notif.get("Body") or {}
    raw_id = (
        notif.get("DeviceId")
        or notif.get("deviceId")
        or body.get("DeviceId")
        or body.get("deviceId")
        or body.get("mac")
        or body.get("macId")
    )
    device_id = clean_device_id(raw_id)

    if notif_type == "LiveFeedEvent":
        return ResideoLiveFeed(
            device_id=device_id,
            property_name=body.get("PropertyName"),
            value=body.get("Value"),
            subscription_expiration=body.get("SubscriptionExpiration"),
        )
    if notif_type in ("ChangeRequestSuccess", "ChangeRequestFailure"):
        return ResideoChangeConfirm(
            device_id=device_id,
            transaction_id=body.get("TransactionId"),
            change_name=body.get("ChangeName"),
            change_direction=body.get("ChangeDirection"),
            success=notif_type == "ChangeRequestSuccess",
        )
    if notif_type:
        # Fallback for device alerts, alarms, or leak detector events
        prop_name = body.get("PropertyName") or notif_type
        val = body.get("Value") if "Value" in body else body
        return ResideoLiveFeed(
            device_id=device_id,
            property_name=prop_name,
            value=val,
            subscription_expiration=body.get("SubscriptionExpiration"),
        )
    return None


