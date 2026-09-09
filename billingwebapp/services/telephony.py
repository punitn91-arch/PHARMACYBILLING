"""Provider-neutral safety primitives for future inbound clinic calls.

This module deliberately contains no Flask, network, or telephony-provider
code. A provider adapter can use these types to verify its own signed webhook
before it parses any caller-controlled payload. The helpers never log caller
numbers, request bodies, signatures, or secrets.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass, field
from typing import Any, Generic, Mapping, Optional, Protocol, TypeVar, Union, runtime_checkable


T = TypeVar("T")

CANONICAL_SIGNATURE_VERSION = "clinic-telephony-v1"
MAX_CALLER_INPUT_LENGTH = 32
MAX_REQUEST_TARGET_LENGTH = 2048
MAX_EVENT_IDENTIFIER_LENGTH = 128
MAX_EVENT_TYPE_LENGTH = 64
MAX_TIMESTAMP_SECONDS = 4_102_444_800  # 2100-01-01 UTC
MAX_REPLAY_WINDOW_SECONDS = 3_600

_PROVIDER_NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_EVENT_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_EVENT_TYPE_RE = re.compile(r"[a-z0-9][a-z0-9._:-]{0,63}")
_HTTP_METHOD_RE = re.compile(r"[A-Z]{3,12}")


@dataclass(frozen=True)
class TelephonyError:
    """A stable caller-safe error which never embeds untrusted input."""

    code: str
    public_message: str
    status_code: int


@dataclass(frozen=True)
class TelephonyResult(Generic[T]):
    """Explicit success/failure result for untrusted telephony input."""

    ok: bool
    value: Optional[T] = None
    error: Optional[TelephonyError] = None

    @classmethod
    def success(cls, value: Optional[T] = None) -> "TelephonyResult[T]":
        return cls(ok=True, value=value, error=None)

    @classmethod
    def failure(cls, error: TelephonyError) -> "TelephonyResult[T]":
        return cls(ok=False, value=None, error=error)


@dataclass(frozen=True)
class TelephonyEvent:
    """Minimal normalized facts emitted by a verified provider adapter.

    ``caller_e164`` is hidden from the default representation. Raw webhook
    bodies, transcripts, recordings, OTPs, and signatures do not belong here.
    """

    provider: str
    provider_call_id: str
    event_id: str
    event_type: str
    occurred_at: int
    caller_e164: str = field(repr=False)


@runtime_checkable
class TelephonyProvider(Protocol):
    """Narrow contract implemented by a future provider-specific adapter."""

    name: str

    def verify_request(
        self,
        *,
        method: str,
        request_target: str,
        body: bytes,
        headers: Mapping[str, str],
        now: int,
    ) -> TelephonyResult[None]:
        """Verify a request without logging its credentials or payload."""

    def parse_event(self, *, body: bytes, headers: Mapping[str, str]) -> TelephonyResult[TelephonyEvent]:
        """Parse an event only after verification succeeds."""

    def render_reply(self, *, text: str, language: str, collect_input: bool) -> TelephonyResult[str]:
        """Return provider-specific response markup without doing network I/O."""


def _failure(code: str, public_message: str, status_code: int) -> TelephonyResult[Any]:
    return TelephonyResult.failure(
        TelephonyError(code=code, public_message=public_message, status_code=status_code)
    )


def _coerce_epoch_seconds(value: Union[str, int]) -> TelephonyResult[int]:
    """Accept bounded Unix epoch seconds, never locale-specific timestamps."""

    if isinstance(value, bool):
        return _failure("invalid_timestamp", "The request timestamp is invalid.", 400)
    if isinstance(value, int):
        seconds = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value.strip() or ""):
        seconds = int(value.strip())
    else:
        return _failure("invalid_timestamp", "The request timestamp is invalid.", 400)
    if seconds < 0 or seconds > MAX_TIMESTAMP_SECONDS:
        return _failure("invalid_timestamp", "The request timestamp is invalid.", 400)
    return TelephonyResult.success(seconds)


def normalize_indian_caller_e164(raw_caller: object) -> TelephonyResult[str]:
    """Normalize common Indian caller formats to a bounded E.164 value.

    This accepts a ten-digit national number, a domestic ``0`` prefix, or an
    Indian ``91``/``+91``/``0091`` prefix. It permits landline-looking callers
    as well as mobile callers; an OTP flow must separately require an
    SMS-capable mobile number.
    """

    if not isinstance(raw_caller, str):
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)
    raw_value = raw_caller.strip()
    if not raw_value or len(raw_value) > MAX_CALLER_INPUT_LENGTH:
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)

    # Do not use ``str.isdigit``: it accepts Unicode lookalike numerals.
    if not re.fullmatch(r"\+?[0-9][0-9 ()\-.]*", raw_value):
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)
    compact = re.sub(r"[ ()\-.]", "", raw_value)
    if compact.startswith("+"):
        compact = compact[1:]
    elif compact.startswith("00"):
        compact = compact[2:]
    if not re.fullmatch(r"[0-9]+", compact):
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)

    if len(compact) == 10:
        national_number = compact
    elif len(compact) == 11 and compact.startswith("0"):
        national_number = compact[1:]
    elif len(compact) == 12 and compact.startswith("91"):
        national_number = compact[2:]
    else:
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)
    if national_number[0] == "0" or not any(digit != "0" for digit in national_number):
        return _failure("invalid_caller", "A valid Indian caller number is required.", 400)
    return TelephonyResult.success("+91{}".format(national_number))


def _normalize_event_field(
    value: object,
    *,
    pattern: Any,
    maximum_length: int,
    casefold: bool = False,
) -> TelephonyResult[str]:
    if not isinstance(value, str):
        return _failure("invalid_event_field", "The telephony event is invalid.", 400)
    cleaned = value.strip().casefold() if casefold else value.strip()
    if not cleaned or len(cleaned) > maximum_length or not pattern.fullmatch(cleaned):
        return _failure("invalid_event_field", "The telephony event is invalid.", 400)
    return TelephonyResult.success(cleaned)


def build_telephony_event(
    *,
    provider: object,
    provider_call_id: object,
    event_id: object,
    event_type: object,
    caller: object,
    occurred_at: Union[str, int],
) -> TelephonyResult[TelephonyEvent]:
    """Build a bounded event without retaining a raw provider payload."""

    provider_result = _normalize_event_field(
        provider, pattern=_PROVIDER_NAME_RE, maximum_length=64, casefold=True
    )
    if not provider_result.ok:
        return TelephonyResult.failure(provider_result.error)  # type: ignore[arg-type]
    call_id_result = _normalize_event_field(
        provider_call_id,
        pattern=_EVENT_IDENTIFIER_RE,
        maximum_length=MAX_EVENT_IDENTIFIER_LENGTH,
    )
    if not call_id_result.ok:
        return TelephonyResult.failure(call_id_result.error)  # type: ignore[arg-type]
    event_id_result = _normalize_event_field(
        event_id, pattern=_EVENT_IDENTIFIER_RE, maximum_length=MAX_EVENT_IDENTIFIER_LENGTH
    )
    if not event_id_result.ok:
        return TelephonyResult.failure(event_id_result.error)  # type: ignore[arg-type]
    event_type_result = _normalize_event_field(
        event_type, pattern=_EVENT_TYPE_RE, maximum_length=MAX_EVENT_TYPE_LENGTH, casefold=True
    )
    if not event_type_result.ok:
        return TelephonyResult.failure(event_type_result.error)  # type: ignore[arg-type]
    caller_result = normalize_indian_caller_e164(caller)
    if not caller_result.ok:
        return TelephonyResult.failure(caller_result.error)  # type: ignore[arg-type]
    timestamp_result = _coerce_epoch_seconds(occurred_at)
    if not timestamp_result.ok:
        return TelephonyResult.failure(timestamp_result.error)  # type: ignore[arg-type]

    return TelephonyResult.success(
        TelephonyEvent(
            provider=provider_result.value or "",
            provider_call_id=call_id_result.value or "",
            event_id=event_id_result.value or "",
            event_type=event_type_result.value or "",
            occurred_at=timestamp_result.value or 0,
            caller_e164=caller_result.value or "",
        )
    )


def canonical_signed_request(
    *,
    method: object,
    request_target: object,
    timestamp: Union[str, int],
    body: object,
) -> TelephonyResult[bytes]:
    """Return exact versioned bytes used by the generic HMAC verifier.

    ``request_target`` is the original path plus query string. The raw body is
    represented by its SHA-256 digest, avoiding ambiguous canonical bytes.
    """

    if not isinstance(method, str):
        return _failure("invalid_request", "The signed request is invalid.", 400)
    normalized_method = method.strip().upper()
    if not _HTTP_METHOD_RE.fullmatch(normalized_method):
        return _failure("invalid_request", "The signed request is invalid.", 400)
    if not isinstance(request_target, str):
        return _failure("invalid_request", "The signed request is invalid.", 400)
    target = request_target.strip()
    if (
        not target
        or len(target) > MAX_REQUEST_TARGET_LENGTH
        or not target.startswith("/")
        or "\r" in target
        or "\n" in target
    ):
        return _failure("invalid_request", "The signed request is invalid.", 400)
    timestamp_result = _coerce_epoch_seconds(timestamp)
    if not timestamp_result.ok:
        return TelephonyResult.failure(timestamp_result.error)  # type: ignore[arg-type]
    if not isinstance(body, (bytes, bytearray)):
        return _failure("invalid_request", "The signed request is invalid.", 400)

    canonical = "\n".join(
        (
            CANONICAL_SIGNATURE_VERSION,
            normalized_method,
            target,
            str(timestamp_result.value),
            hashlib.sha256(bytes(body)).hexdigest(),
        )
    ).encode("utf-8")
    return TelephonyResult.success(canonical)


def _secret_bytes(secret: object) -> TelephonyResult[bytes]:
    if isinstance(secret, str):
        secret_bytes = secret.encode("utf-8")
    elif isinstance(secret, bytes):
        secret_bytes = secret
    else:
        return _failure("verifier_not_configured", "The telephony verifier is unavailable.", 500)
    if not secret_bytes or len(secret_bytes) > 4096:
        return _failure("verifier_not_configured", "The telephony verifier is unavailable.", 500)
    return TelephonyResult.success(secret_bytes)


def sign_hmac_request(
    *,
    secret: object,
    method: object,
    request_target: object,
    timestamp: Union[str, int],
    body: object,
) -> TelephonyResult[str]:
    """Generate a canonical ``sha256=...`` signature for adapter tests."""

    secret_result = _secret_bytes(secret)
    if not secret_result.ok:
        return TelephonyResult.failure(secret_result.error)  # type: ignore[arg-type]
    canonical_result = canonical_signed_request(
        method=method,
        request_target=request_target,
        timestamp=timestamp,
        body=body,
    )
    if not canonical_result.ok:
        return TelephonyResult.failure(canonical_result.error)  # type: ignore[arg-type]
    digest = hmac.new(
        secret_result.value or b"", canonical_result.value or b"", hashlib.sha256
    ).hexdigest()
    return TelephonyResult.success("sha256={}".format(digest))


def verify_hmac_signed_request(
    *,
    secret: object,
    method: object,
    request_target: object,
    timestamp: Union[str, int],
    signature: object,
    body: object,
    now: Union[str, int],
    replay_window_seconds: object,
) -> TelephonyResult[None]:
    """Verify HMAC authentication plus a bounded timestamp replay window.

    This check is deliberately not a durable replay store. A real provider
    adapter must also persist a unique event ID to deduplicate retries.
    """

    timestamp_result = _coerce_epoch_seconds(timestamp)
    if not timestamp_result.ok:
        return TelephonyResult.failure(timestamp_result.error)  # type: ignore[arg-type]
    now_result = _coerce_epoch_seconds(now)
    if not now_result.ok:
        return TelephonyResult.failure(now_result.error)  # type: ignore[arg-type]
    if isinstance(replay_window_seconds, bool):
        return _failure("invalid_replay_window", "The telephony verifier is unavailable.", 500)
    try:
        replay_window = int(replay_window_seconds)
    except (TypeError, ValueError):
        return _failure("invalid_replay_window", "The telephony verifier is unavailable.", 500)
    if replay_window < 1 or replay_window > MAX_REPLAY_WINDOW_SECONDS:
        return _failure("invalid_replay_window", "The telephony verifier is unavailable.", 500)

    timestamp_seconds = timestamp_result.value or 0
    current_seconds = now_result.value or 0
    if timestamp_seconds < current_seconds - replay_window:
        return _failure("expired_timestamp", "This telephony request has expired.", 401)
    if timestamp_seconds > current_seconds + replay_window:
        return _failure("future_timestamp", "This telephony request is not yet valid.", 401)
    if not isinstance(signature, str):
        return _failure("invalid_signature", "The telephony signature is invalid.", 401)
    supplied_signature = signature.strip()
    if supplied_signature.casefold().startswith("sha256="):
        supplied_signature = supplied_signature.split("=", 1)[1]
    if not re.fullmatch(r"[0-9a-fA-F]{64}", supplied_signature):
        return _failure("invalid_signature", "The telephony signature is invalid.", 401)

    expected_result = sign_hmac_request(
        secret=secret,
        method=method,
        request_target=request_target,
        timestamp=timestamp_seconds,
        body=body,
    )
    if not expected_result.ok:
        return TelephonyResult.failure(expected_result.error)  # type: ignore[arg-type]
    expected_signature = (expected_result.value or "").split("=", 1)[-1]
    if not hmac.compare_digest(expected_signature, supplied_signature.casefold()):
        return _failure("invalid_signature", "The telephony signature is invalid.", 401)
    return TelephonyResult.success()


__all__ = (
    "CANONICAL_SIGNATURE_VERSION",
    "TelephonyError",
    "TelephonyEvent",
    "TelephonyProvider",
    "TelephonyResult",
    "build_telephony_event",
    "canonical_signed_request",
    "normalize_indian_caller_e164",
    "sign_hmac_request",
    "verify_hmac_signed_request",
)
