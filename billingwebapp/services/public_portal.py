"""Delivery and display helpers for public patient portals.

The routes keep OTP persistence and authorization in the database.  This
module only decides how a one-time code is delivered, so a real provider can
be enabled without weakening local development or test behaviour.
"""

from __future__ import annotations

import base64
import logging
import os
import urllib.parse
import urllib.request


LOGGER = logging.getLogger(__name__)


def mask_mobile(mobile):
    digits = "".join(ch for ch in str(mobile or "") if ch.isdigit())
    if len(digits) <= 4:
        return "•" * len(digits)
    return f"{'•' * (len(digits) - 4)}{digits[-4:]}"


def _flag_enabled(value):
    return str(value or "").strip().lower() not in {"", "0", "false", "no", "off"}


def _twilio_recipient(mobile):
    digits = "".join(ch for ch in str(mobile or "") if ch.isdigit())
    if len(digits) == 10:
        digits = f"91{digits}"
    return f"whatsapp:+{digits}" if digits else ""


def send_portal_otp(*, mobile, code, purpose, is_production=False, testing=False):
    """Send an OTP by configured provider.

    Returns ``(sent, development_code, error_message)``.  The code is only
    returned in explicit local/test mode; production never exposes it.
    """
    configured_mode = (os.environ.get("PUBLIC_PORTAL_OTP_MODE") or "").strip().lower()
    mode = configured_mode or ("disabled" if is_production else "development")
    message = (
        f"Your Endo Clinic {purpose.lower()} verification code is {code}. "
        "It expires in 5 minutes. Do not share this code."
    )

    if mode in {"development", "console", "test"}:
        LOGGER.warning("Local-only OTP generated for %s (%s): %s", mask_mobile(mobile), purpose, code)
        show_code = bool(testing or _flag_enabled(os.environ.get("PUBLIC_PORTAL_SHOW_DEV_OTP")))
        return True, code if show_code else "", ""

    if mode != "twilio_whatsapp":
        return False, "", "OTP delivery is not configured yet. Please contact the clinic."

    account_sid = (os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = (os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    sender = (os.environ.get("TWILIO_WHATSAPP_FROM") or "").strip()
    recipient = _twilio_recipient(mobile)
    if (
        not account_sid
        or not auth_token
        or not sender.startswith("whatsapp:+")
        or account_sid.lower().startswith("acxxxx")
        or auth_token.lower().startswith("your_")
        or not recipient
    ):
        return False, "", "OTP delivery is not configured yet. Please contact the clinic."

    payload = urllib.parse.urlencode({"From": sender, "To": recipient, "Body": message}).encode("utf-8")
    token = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=payload,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # nosec B310 - fixed provider endpoint
            if 200 <= int(getattr(response, "status", 200)) < 300:
                return True, "", ""
    except Exception:
        LOGGER.exception("Unable to deliver public portal OTP through Twilio")
    return False, "", "Unable to send the verification code right now. Please try again later."
