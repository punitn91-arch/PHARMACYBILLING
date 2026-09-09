"""Approved-template, idempotent notification boundary.

Arbitrary message bodies are intentionally unsupported. Production delivery
stays failed-closed until a reviewed provider adapter is configured.
"""

from datetime import datetime
import hashlib
import hmac
import json
import os
import re

try:
    from ..models import NotificationDelivery, NotificationTemplate
    from .public_portal import mask_mobile
except ImportError:  # pragma: no cover
    from models import NotificationDelivery, NotificationTemplate
    from services.public_portal import mask_mobile


ALLOWED_TEMPLATE_VARIABLES = frozenset(
    {
        "patient_name",
        "appointment_no",
        "doctor_name",
        "appointment_date",
        "appointment_time",
        "clinic_name",
        "clinic_phone",
        "location_name",
        "maps_url",
        "report_title",
        "secure_link",
    }
)
_VARIABLE_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


class NotificationError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _hash(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _recipient_fingerprint(recipient, secret):
    return hmac.new(
        str(secret).encode("utf-8"), str(recipient).encode("utf-8"), hashlib.sha256
    ).hexdigest()


def render_approved_template(template, variables):
    requested = set(_VARIABLE_PATTERN.findall(template.body_template or ""))
    if not requested.issubset(ALLOWED_TEMPLATE_VARIABLES):
        raise NotificationError("NOTIFICATION_FAILED", "Template contains unsupported variables", 500)
    supplied = {key: str(value or "") for key, value in (variables or {}).items() if key in ALLOWED_TEMPLATE_VARIABLES}
    missing = requested.difference(supplied)
    if missing:
        raise NotificationError("INVALID_REQUEST", "Required template variables are missing", 400)
    return (template.body_template or "").format_map(supplied)


def send_template_notification(
    db_session,
    *,
    api_client_id,
    event_code,
    channel,
    language,
    recipient,
    variables,
    idempotency_key,
    fingerprint_secret,
    testing=False,
):
    normalized_channel = str(channel or "").upper()
    if normalized_channel not in {"SMS", "WHATSAPP", "EMAIL"}:
        raise NotificationError("INVALID_REQUEST", "Unsupported notification channel", 400)
    raw_idempotency_key = str(idempotency_key or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", raw_idempotency_key):
        raise NotificationError("INVALID_REQUEST", "A valid Idempotency-Key is required", 400)
    key_hash = _hash(raw_idempotency_key)
    request_fingerprint = _hash(
        json.dumps(
            {
                "event_code": str(event_code or "").upper(),
                "channel": normalized_channel,
                "language": str(language or "en").lower(),
                "recipient": str(recipient),
                # A secure document URL is intentionally generated afresh on
                # each attempt. Its secret token must not make an otherwise
                # identical notification retry look like a conflicting body.
                "variables": {
                    key: ("<secure-link>" if key == "secure_link" else value)
                    for key, value in (variables or {}).items()
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    existing = NotificationDelivery.query.filter_by(
        api_client_id=api_client_id,
        idempotency_key_hash=key_hash,
    ).first()
    if existing:
        if existing.request_fingerprint != request_fingerprint:
            raise NotificationError(
                "IDEMPOTENCY_CONFLICT",
                "Idempotency key was already used with a different notification",
                409,
            )
        return existing, True
    template = NotificationTemplate.query.filter_by(
        event_code=str(event_code or "").upper(),
        channel=normalized_channel,
        language=str(language or "en").lower(),
        is_active=True,
    ).first()
    if not template:
        raise NotificationError("NOTIFICATION_FAILED", "Approved notification template was not found", 404)
    rendered = render_approved_template(template, variables)
    # Do not persist the rendered body; it may contain a secure link or patient
    # name. Provider adapters receive it only in memory.
    mode = str(os.environ.get("AI_NOTIFICATION_MODE") or "disabled").lower()
    delivered = bool(testing and mode in {"test", "development"})
    delivery = NotificationDelivery(
        api_client_id=api_client_id,
        template_id=template.id,
        channel=normalized_channel,
        recipient_masked=mask_mobile(recipient),
        recipient_fingerprint=_recipient_fingerprint(recipient, fingerprint_secret),
        idempotency_key_hash=key_hash,
        request_fingerprint=request_fingerprint,
        status="SENT" if delivered else "FAILED",
        provider="TEST" if delivered else None,
        error_code=None if delivered else "PROVIDER_NOT_CONFIGURED",
        sent_at=datetime.utcnow() if delivered else None,
    )
    db_session.add(delivery)
    db_session.flush()
    if not delivered:
        raise NotificationError("NOTIFICATION_FAILED", "Notification provider is not configured", 503)
    return delivery, False
