"""Provider-neutral inbound webhook boundary for the AI call receptionist.

This endpoint format is intentionally a staging harness, not a claim of
compatibility with Exotel, Twilio, or another provider. A future provider
adapter must verify that provider's documented signature and translate its
payload into this small internal event shape before persisting anything.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional

try:
    from ..services.telephony import build_telephony_event, verify_hmac_signed_request
    from ..services.voice_call_sessions import (
        VoiceCallEventConflictError,
        VoiceCallSessionError,
        persist_verified_telephony_event,
    )
except ImportError:  # pragma: no cover - direct app.py execution
    from services.telephony import build_telephony_event, verify_hmac_signed_request
    from services.voice_call_sessions import (
        VoiceCallEventConflictError,
        VoiceCallSessionError,
        persist_verified_telephony_event,
    )


MAX_WEBHOOK_BODY_BYTES = 64 * 1024
MAX_EVENT_AGE_SECONDS = 60 * 60
MAX_EVENT_FUTURE_SKEW_SECONDS = 5 * 60
REQUIRED_EVENT_FIELDS = frozenset(
    {"call_id", "event_id", "event_type", "caller", "occurred_at"}
)


@dataclass(frozen=True)
class TelephonyWebhookResponse:
    """Safe HTTP response data; it deliberately exposes no call identity."""

    payload: Mapping[str, object]
    status_code: int


@dataclass(frozen=True)
class GenericTelephonySecrets:
    """Two independent keys for the generic staging endpoint."""

    webhook_secret: str
    fingerprint_secret: str


def _configured_secrets(
    provider: object, environment: Mapping[str, str]
) -> Optional[GenericTelephonySecrets]:
    """Return the generic staging secret only for its explicit provider route."""

    enabled = (environment.get("VOICE_TELEPHONY_ENABLED") or "").strip().casefold()
    if enabled not in {"1", "true", "yes", "on"}:
        return None
    app_environment = (environment.get("APP_ENV") or "").strip().casefold()
    # This generic HMAC envelope is only a local/staging contract. A live
    # carrier requires its own documented, provider-specific verifier.
    if app_environment in {"production", "prod"}:
        return None
    configured_provider = (environment.get("VOICE_TELEPHONY_PROVIDER") or "").strip().casefold()
    normalized_provider = str(provider or "").strip().casefold()
    webhook_secret = (environment.get("VOICE_TELEPHONY_WEBHOOK_SECRET") or "").strip()
    fingerprint_secret = (environment.get("VOICE_TELEPHONY_FINGERPRINT_SECRET") or "").strip()
    if (
        not configured_provider
        or configured_provider != normalized_provider
        or len(webhook_secret.encode("utf-8")) < 16
        or len(fingerprint_secret.encode("utf-8")) < 16
    ):
        return None
    return GenericTelephonySecrets(
        webhook_secret=webhook_secret,
        fingerprint_secret=fingerprint_secret,
    )


def _safe_failure(code: str, status_code: int) -> TelephonyWebhookResponse:
    # Keep errors intentionally non-specific: public webhook callers must not
    # learn whether a provider/secret/call identifier is configured.
    return TelephonyWebhookResponse({"accepted": False, "error": code}, status_code)


def _parse_verified_event(raw_body: bytes, provider: str):
    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, TypeError):
        return None
    if not isinstance(parsed, dict) or set(parsed) != REQUIRED_EVENT_FIELDS:
        return None
    event_result = build_telephony_event(
        provider=provider,
        provider_call_id=parsed.get("call_id"),
        event_id=parsed.get("event_id"),
        event_type=parsed.get("event_type"),
        caller=parsed.get("caller"),
        occurred_at=parsed.get("occurred_at"),
    )
    return event_result.value if event_result.ok else None


def _event_time_is_plausible(*, event_occurred_at: int, signed_timestamp: str) -> bool:
    """Keep a signed staging event close to its signed request time."""

    try:
        request_seconds = int(signed_timestamp)
    except (TypeError, ValueError):
        return False
    return (
        request_seconds - MAX_EVENT_AGE_SECONDS
        <= event_occurred_at
        <= request_seconds + MAX_EVENT_FUTURE_SKEW_SECONDS
    )


def handle_generic_telephony_webhook(
    *,
    provider: object,
    method: str,
    request_target: str,
    raw_body: bytes,
    headers: Mapping[str, str],
    db_session: Any,
    environment: Optional[Mapping[str, str]] = None,
    now_epoch: Optional[int] = None,
    now_datetime: Optional[datetime] = None,
) -> TelephonyWebhookResponse:
    """Authenticate, validate, and idempotently persist one staging event.

    The caller must pass the raw HTTP bytes. Signature validation happens
    before JSON decoding, and the body admits only five metadata fields. A
    real provider adapter will be introduced separately after its account and
    documented signing scheme are chosen.
    """

    if not isinstance(raw_body, (bytes, bytearray)) or len(raw_body) > MAX_WEBHOOK_BODY_BYTES:
        return _safe_failure("invalid_request", 400)
    environ = environment if environment is not None else os.environ
    normalized_provider = str(provider or "").strip().casefold()
    secrets = _configured_secrets(normalized_provider, environ)
    if not secrets:
        return _safe_failure("not_found", 404)

    timestamp = headers.get("X-Clinic-Timestamp") or headers.get("x-clinic-timestamp") or ""
    signature = headers.get("X-Clinic-Signature") or headers.get("x-clinic-signature") or ""
    verification = verify_hmac_signed_request(
        secret=secrets.webhook_secret,
        method=method,
        request_target=request_target,
        timestamp=timestamp,
        signature=signature,
        body=bytes(raw_body),
        now=int(time.time()) if now_epoch is None else now_epoch,
        replay_window_seconds=300,
    )
    if not verification.ok:
        return _safe_failure("unauthorized", 401)

    event = _parse_verified_event(bytes(raw_body), normalized_provider)
    if event is None or not _event_time_is_plausible(
        event_occurred_at=event.occurred_at,
        signed_timestamp=timestamp,
    ):
        return _safe_failure("invalid_event", 400)
    try:
        receipt = persist_verified_telephony_event(
            db_session=db_session,
            event=event,
            raw_body=bytes(raw_body),
            fingerprint_secret=secrets.fingerprint_secret,
            now=now_datetime,
        )
    except VoiceCallEventConflictError:
        return _safe_failure("event_conflict", 409)
    except VoiceCallSessionError:
        return _safe_failure("temporarily_unavailable", 503)

    return TelephonyWebhookResponse(
        {
            "accepted": True,
            "duplicate": receipt.duplicate,
            "call_status": receipt.call_status,
        },
        200 if receipt.duplicate else 202,
    )
