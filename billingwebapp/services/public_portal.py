"""Delivery and display helpers for public patient portals.

The routes keep OTP persistence and authorization in the database.  This
module only decides how a one-time code is delivered, so a real provider can
be enabled without weakening local development or test behaviour.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
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


def _indian_mobile_with_country_code(mobile):
    digits = "".join(ch for ch in str(mobile or "") if ch.isdigit())
    if len(digits) == 10:
        digits = f"91{digits}"
    if len(digits) != 12 or not digits.startswith("91"):
        return ""
    return digits


def _twilio_recipient(mobile):
    recipient = _indian_mobile_with_country_code(mobile)
    return f"whatsapp:+{recipient}" if recipient else ""


def _msg91_sms_config():
    """Read the MSG91 Flow credentials without ever logging secret values."""
    authkey = (os.environ.get("MSG91_AUTH_KEY") or os.environ.get("MSG91_AUTHKEY") or "").strip()
    template_id = (os.environ.get("MSG91_TEMPLATE_ID") or os.environ.get("MSG91_FLOW_ID") or "").strip()
    variable_name = (os.environ.get("MSG91_OTP_VARIABLE") or "otp").strip()
    sender_id = (os.environ.get("MSG91_SENDER_ID") or "").strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,39}", variable_name or ""):
        variable_name = "otp"
    if (
        not authkey
        or not template_id
        or authkey.lower().startswith(("your_", "replace_", "msg91_"))
        or template_id.lower().startswith(("your_", "replace_", "template_", "flow_"))
    ):
        return None
    return {
        "authkey": authkey,
        "template_id": template_id,
        "otp_variable": variable_name,
        "sender_id": sender_id,
    }


def _send_msg91_sms(*, mobile, code):
    """Submit an OTP to MSG91's approved SMS Flow template.

    Indian SMS rules require the exact DLT-approved template to be created in
    MSG91 first.  The variable name below must match that MSG91 template, for
    example ``##otp##`` in the template maps to ``MSG91_OTP_VARIABLE=otp``.
    """
    config = _msg91_sms_config()
    recipient = _indian_mobile_with_country_code(mobile)
    if not config or not recipient:
        return False, "SMS OTP delivery is not configured yet. Please contact the clinic."

    recipient_data = {
        "mobiles": recipient,
        config["otp_variable"]: str(code),
    }
    payload = {
        "template_id": config["template_id"],
        "short_url": "0",
        "recipients": [recipient_data],
    }
    # A sender is normally linked to the approved MSG91 template.  Include it
    # only for customers whose template was explicitly configured as FromAPI.
    if config["sender_id"]:
        payload["sender"] = config["sender_id"]

    request = urllib.request.Request(
        "https://control.msg91.com/api/v5/flow",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "accept": "application/json",
            "authkey": config["authkey"],
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # nosec B310 - fixed MSG91 endpoint
            status = int(getattr(response, "status", 200))
            raw_body = response.read().decode("utf-8", errors="replace")
            if not 200 <= status < 300:
                return False, "Unable to send the verification SMS right now. Please try again later."
            try:
                body = json.loads(raw_body) if raw_body else {}
            except (TypeError, ValueError):
                body = {}
            if isinstance(body, dict) and str(body.get("type") or "").strip().lower() == "error":
                LOGGER.warning("MSG91 rejected public portal OTP for %s", mask_mobile(mobile))
                return False, "Unable to send the verification SMS right now. Please try again later."
            return True, ""
    except Exception:
        LOGGER.exception("Unable to deliver public portal OTP through MSG91")
    return False, "Unable to send the verification SMS right now. Please try again later."


def _twofactor_sms_config():
    """Return the configured 2Factor key without exposing it in logs.

    The legacy 2Factor custom-OTP endpoint uses the approved template that is
    configured in the 2Factor account.  It receives the generated six-digit
    code in the URL path, so the secret must stay in an environment variable
    and must never be logged.
    """
    api_key = (
        os.environ.get("TWOFACTOR_API_KEY")
        or os.environ.get("TWO_FACTOR_API_KEY")
        or os.environ.get("TFACTOR_API_KEY")
        or ""
    ).strip()
    if not api_key or api_key.lower().startswith(("your_", "replace_", "twofactor_", "2factor_")):
        return None
    return {"api_key": api_key}


def _send_twofactor_sms(*, mobile, code):
    """Submit our server-generated OTP through 2Factor's approved SMS template.

    OTP generation, expiry, hashing, retry limits, and verification remain
    local to this application.  2Factor is used only as the SMS transport.
    """
    config = _twofactor_sms_config()
    recipient = _indian_mobile_with_country_code(mobile)
    otp_code = str(code or "").strip()
    if not config or not recipient or not re.fullmatch(r"\d{6}", otp_code):
        return False, "SMS OTP delivery is not configured yet. Please contact the clinic."

    # This is 2Factor's documented custom-code endpoint. Quote each path
    # segment so a future key format cannot alter the fixed provider URL.
    encoded_key = urllib.parse.quote(config["api_key"], safe="")
    request = urllib.request.Request(
        f"https://2factor.in/API/V1/{encoded_key}/SMS/{recipient}/{otp_code}",
        data=None,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # nosec B310 - fixed 2Factor endpoint
            status = int(getattr(response, "status", 200))
            raw_body = response.read().decode("utf-8", errors="replace")
            if not 200 <= status < 300:
                return False, "Unable to send the verification SMS right now. Please try again later."
            try:
                body = json.loads(raw_body) if raw_body else {}
            except (TypeError, ValueError):
                body = {}
            if isinstance(body, dict):
                provider_status = str(body.get("Status") or body.get("status") or "").strip().lower()
                if provider_status in {"error", "failed", "failure"}:
                    LOGGER.warning("2Factor rejected public portal OTP for %s", mask_mobile(mobile))
                    return False, "Unable to send the verification SMS right now. Please try again later."
            return True, ""
    except Exception:
        # The API key is a path segment for this provider. Do not log the
        # exception object or request URL, which could accidentally reveal it.
        LOGGER.warning("Unable to deliver public portal OTP through 2Factor for %s", mask_mobile(mobile))
    return False, "Unable to send the verification SMS right now. Please try again later."


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

    if mode in {"msg91", "msg91_sms", "sms"}:
        sent, error_message = _send_msg91_sms(mobile=mobile, code=code)
        return sent, "", error_message

    if mode in {"twofactor", "twofactor_sms", "2factor", "2factor_sms"}:
        sent, error_message = _send_twofactor_sms(mobile=mobile, code=code)
        return sent, "", error_message

    if mode != "twilio_whatsapp":
        return False, "", "SMS OTP delivery is not configured yet. Please contact the clinic."

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
