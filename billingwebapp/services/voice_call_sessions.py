"""Durable, minimal-retention state for verified inbound telephony events.

This module is the persistence boundary between a provider adapter and the
clinic application. It stores only metadata necessary to make provider retries
idempotent. It never stores raw request bodies, caller audio, transcripts,
OTP values, report data, or the caller's full phone number.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

try:
    from ..models import VoiceCall, VoiceCallEvent
    from .telephony import TelephonyEvent
except ImportError:  # pragma: no cover - direct app.py execution
    from models import VoiceCall, VoiceCallEvent
    from services.telephony import TelephonyEvent


_SAFE_CONTEXT_KEYS = frozenset(
    {
        "selected_date",
        "preferred_period",
        "requested_location",
        "requested_doctor",
        "human_handoff_requested",
    }
)
_SAFE_CONTEXT_TEXT = re.compile(r"[A-Za-z0-9 _.,:/-]{1,80}")
_CALL_START_EVENTS = frozenset({"call.initiated", "call.ringing", "call.answered", "call.connected"})
_CALL_END_EVENTS = frozenset({"call.completed", "call.ended", "call.hangup", "call.cancelled"})
_CALL_FAILURE_EVENTS = frozenset({"call.failed", "call.busy", "call.no_answer"})
_TERMINAL_CALL_STATUSES = frozenset({"COMPLETED", "FAILED"})


@dataclass(frozen=True)
class VoiceCallEventReceipt:
    """Result returned to an authenticated provider adapter.

    The internal call primary key is deliberately omitted. Provider-facing
    routes need only know whether the event was accepted or was a retry.
    """

    duplicate: bool
    call_status: str


class VoiceCallSessionError(ValueError):
    """Raised for an invalid server-side telephony persistence request."""


class VoiceCallEventConflictError(VoiceCallSessionError):
    """A provider reused an event identifier for different immutable facts."""


def caller_fingerprint(*, caller_e164: str, secret: object) -> str:
    """Return a keyed, non-reversible caller correlation value.

    A plain SHA-256 of a phone number is guessable. HMAC ties the fingerprint
    to the application's private webhook secret instead.
    """

    if isinstance(secret, str):
        secret_bytes = secret.encode("utf-8")
    elif isinstance(secret, bytes):
        secret_bytes = secret
    else:
        raise VoiceCallSessionError("Telephony session secret is unavailable.")
    if len(secret_bytes) < 16:
        raise VoiceCallSessionError("Telephony session secret is unavailable.")
    if not isinstance(caller_e164, str) or not re.fullmatch(r"\+91[0-9]{10}", caller_e164):
        raise VoiceCallSessionError("Telephony caller is invalid.")
    return hmac.new(
        secret_bytes,
        ("clinic-voice-caller-v1:" + caller_e164).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def event_fingerprint(*, event: TelephonyEvent, caller_fingerprint_value: str, secret: object) -> str:
    """Return a keyed fingerprint of normalized event facts only.

    Provider webhooks can include a caller number in their raw body. A plain
    SHA-256 body hash would therefore be correlatable offline. This HMAC covers
    just the validated facts we need for duplicate-conflict detection and keeps
    the raw body out of the database.
    """

    if isinstance(secret, str):
        secret_bytes = secret.encode("utf-8")
    elif isinstance(secret, bytes):
        secret_bytes = secret
    else:
        raise VoiceCallSessionError("Telephony session secret is unavailable.")
    if len(secret_bytes) < 16:
        raise VoiceCallSessionError("Telephony session secret is unavailable.")
    if not isinstance(caller_fingerprint_value, str) or not re.fullmatch(
        r"[0-9a-f]{64}", caller_fingerprint_value
    ):
        raise VoiceCallSessionError("Telephony event is invalid.")
    canonical_facts = "\x1f".join(
        (
            "clinic-voice-event-v1",
            event.provider,
            event.provider_call_id,
            event.event_id,
            event.event_type,
            str(event.occurred_at),
            caller_fingerprint_value,
        )
    )
    return hmac.new(secret_bytes, canonical_facts.encode("utf-8"), hashlib.sha256).hexdigest()


def safe_call_context(context: object) -> str:
    """Serialize only a small, non-patient subset of conversation context.

    This deliberately rejects caller name, mobile, OTP, report, appointment,
    and free-form transcript values. Protected state remains in existing
    server-side booking/OTP records rather than a telephony context blob.
    """

    if context in (None, {}):
        return "{}"
    if not isinstance(context, Mapping):
        raise VoiceCallSessionError("Voice call context is invalid.")
    if set(context) - _SAFE_CONTEXT_KEYS:
        raise VoiceCallSessionError("Voice call context contains an unsupported field.")

    normalized = {}
    for key, value in context.items():
        if key == "selected_date":
            if isinstance(value, datetime):
                raise VoiceCallSessionError("Voice call context date is invalid.")
            if isinstance(value, date):
                normalized[key] = value.isoformat()
                continue
            if isinstance(value, str):
                try:
                    normalized[key] = date.fromisoformat(value).isoformat()
                    continue
                except ValueError:
                    pass
            raise VoiceCallSessionError("Voice call context date is invalid.")
        if key == "human_handoff_requested":
            if not isinstance(value, bool):
                raise VoiceCallSessionError("Voice call context flag is invalid.")
            normalized[key] = value
            continue
        if not isinstance(value, str):
            raise VoiceCallSessionError("Voice call context is invalid.")
        cleaned = value.strip()
        if not _SAFE_CONTEXT_TEXT.fullmatch(cleaned):
            raise VoiceCallSessionError("Voice call context is invalid.")
        normalized[key] = cleaned

    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _status_after_event(event_type: str) -> Optional[str]:
    normalized = (event_type or "").strip().casefold()
    if normalized in _CALL_END_EVENTS:
        return "COMPLETED"
    if normalized in _CALL_FAILURE_EVENTS:
        return "FAILED"
    if normalized in _CALL_START_EVENTS:
        return "IN_PROGRESS"
    return None


def _event_datetime(event: TelephonyEvent) -> datetime:
    # TelephonyEvent already accepts only bounded Unix seconds. Keep dates
    # naive UTC like the existing SQLAlchemy models.
    return datetime.utcfromtimestamp(event.occurred_at)


def _existing_event(db_session: Any, event: TelephonyEvent) -> Optional[VoiceCallEvent]:
    return db_session.query(VoiceCallEvent).filter_by(
        provider=event.provider,
        provider_event_id=event.event_id,
    ).first()


def _receipt_for_existing_event(
    *, db_session: Any, existing_event: VoiceCallEvent, event_fingerprint_value: str
) -> VoiceCallEventReceipt:
    """Return a retry receipt only when event ID and immutable facts agree."""

    stored_fingerprint = getattr(existing_event, "event_fingerprint", None)
    if not isinstance(stored_fingerprint, str) or not hmac.compare_digest(
        stored_fingerprint, event_fingerprint_value
    ):
        raise VoiceCallEventConflictError("Telephony event identifier conflict.")
    existing_call = db_session.query(VoiceCall).filter_by(id=existing_event.voice_call_id).first()
    return VoiceCallEventReceipt(
        duplicate=True,
        call_status=(getattr(existing_call, "status", "RECEIVED") or "RECEIVED").upper(),
    )


def _update_call_from_event(*, call: VoiceCall, event: TelephonyEvent, event_time: datetime) -> None:
    """Apply one event without allowing delayed webhooks to regress a call.

    Provider delivery order is not reliable. We keep ``last_event_at``
    monotonic, preserve a terminal outcome, and calculate duration from
    provider event times rather than HTTP receipt times.
    """

    normalized_type = (event.event_type or "").strip().casefold()
    current_status = (call.status or "RECEIVED").strip().upper() or "RECEIVED"
    last_event_at = call.last_event_at
    is_older = bool(last_event_at and event_time < last_event_at)

    # A late but genuine start event improves duration accuracy without
    # reopening a completed call.
    if normalized_type in _CALL_START_EVENTS and (
        not call.started_at or event_time < call.started_at
    ):
        call.started_at = event_time

    if not is_older and current_status not in _TERMINAL_CALL_STATUSES:
        next_status = _status_after_event(normalized_type)
        if next_status:
            call.status = next_status
            if next_status in _TERMINAL_CALL_STATUSES:
                call.ended_at = event_time

    if not last_event_at or event_time > last_event_at:
        call.last_event_at = event_time

    if call.ended_at and call.started_at:
        call.duration_seconds = max(0, int((call.ended_at - call.started_at).total_seconds()))


def persist_verified_telephony_event(
    *,
    db_session: Any,
    event: TelephonyEvent,
    raw_body: bytes,
    fingerprint_secret: object,
    now: Optional[datetime] = None,
) -> VoiceCallEventReceipt:
    """Persist one verified provider event and make retries harmless.

    Callers must verify the provider signature *before* calling this function.
    The unique `(provider, provider_event_id)` constraint protects against both
    ordinary retries and concurrent webhook deliveries.
    """

    if not isinstance(event, TelephonyEvent):
        raise VoiceCallSessionError("Telephony event is invalid.")
    if not isinstance(raw_body, (bytes, bytearray)) or len(raw_body) > 64 * 1024:
        raise VoiceCallSessionError("Telephony event payload is invalid.")
    current_time = now or datetime.utcnow()

    fingerprint = caller_fingerprint(caller_e164=event.caller_e164, secret=fingerprint_secret)
    event_fingerprint_value = event_fingerprint(
        event=event,
        caller_fingerprint_value=fingerprint,
        secret=fingerprint_secret,
    )
    event_time = _event_datetime(event)

    # The first attempt handles the normal path. One retry closes the small
    # race where different event IDs for the same newly-created call arrive at
    # exactly the same time and contend on the unique call constraint.
    for attempt in range(2):
        existing_event = _existing_event(db_session, event)
        if existing_event:
            return _receipt_for_existing_event(
                db_session=db_session,
                existing_event=existing_event,
                event_fingerprint_value=event_fingerprint_value,
            )
        try:
            call = db_session.query(VoiceCall).filter_by(
                provider=event.provider,
                provider_call_id=event.provider_call_id,
            ).with_for_update().first()
            if not call:
                call = VoiceCall(
                    provider=event.provider,
                    provider_call_id=event.provider_call_id,
                    caller_fingerprint=fingerprint,
                    caller_last4=event.caller_e164[-4:],
                    status="RECEIVED",
                    started_at=event_time,
                )
                db_session.add(call)
                db_session.flush()

            _update_call_from_event(call=call, event=event, event_time=event_time)
            db_session.add(
                VoiceCallEvent(
                    voice_call_id=call.id,
                    provider=event.provider,
                    provider_event_id=event.event_id,
                    event_type=event.event_type,
                    occurred_at=event_time,
                    event_fingerprint=event_fingerprint_value,
                    processing_status="ACCEPTED",
                    processed_at=current_time,
                )
            )
            db_session.commit()
            return VoiceCallEventReceipt(duplicate=False, call_status=call.status)
        except IntegrityError:
            db_session.rollback()
            existing_event = _existing_event(db_session, event)
            if existing_event:
                return _receipt_for_existing_event(
                    db_session=db_session,
                    existing_event=existing_event,
                    event_fingerprint_value=event_fingerprint_value,
                )
            if attempt == 0:
                continue
            raise VoiceCallSessionError("Unable to record the telephony event.")
        except SQLAlchemyError as error:
            db_session.rollback()
            raise VoiceCallSessionError("Unable to record the telephony event.") from error

    raise VoiceCallSessionError("Unable to record the telephony event.")
