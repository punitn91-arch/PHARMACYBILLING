"""Typed, Flask-independent policy boundary for a future voice call agent.

This module deliberately knows nothing about Flask, SQLAlchemy, phone
providers, or speech services. It only allow-lists clinic tools, requires an
already verified caller for protected actions, and makes patient details unsafe
to put in spoken or logged summaries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
import re
from types import MappingProxyType
from typing import Mapping, Optional, Protocol, Tuple, Union


SafeFieldValue = Union[str, int, float, bool, date, None]


class PublicCallAction(str, Enum):
    """Read-only facts that can be discussed before caller verification."""

    CLINIC_INFORMATION = "clinic_information"
    DOCTOR_SCHEDULE = "doctor_schedule"
    APPOINTMENT_AVAILABILITY = "appointment_availability"
    APPOINTMENT_FEES = "appointment_fees"


class ProtectedCallAction(str, Enum):
    """Patient-specific actions that always need a verified OTP session."""

    LAB_REPORT_STATUS = "lab_report_status"
    LAB_REPORT_DELIVERY = "lab_report_delivery"
    PATIENT_APPOINTMENT_STATUS = "patient_appointment_status"


class AuthorizationStatus(str, Enum):
    """The only decisions the conversation layer may act on."""

    ALLOWED = "allowed"
    OTP_REQUIRED = "otp_required"
    DENIED = "denied"


@dataclass(frozen=True)
class CallToolRequest:
    """Untrusted tool input produced by an LLM/function-call adapter.

    ``name`` deliberately remains a raw object until policy validation. The
    policy converts it to an enum only after it has been allow-listed.
    """

    name: object
    arguments: object = field(default_factory=dict)


@dataclass(frozen=True)
class CallAuthorizationContext:
    """OTP/session state supplied by a trusted application adapter.

    ``verified_caller_ref`` must be an opaque server-side reference, never a
    mobile number, OTP, booking reference, or patient identifier. It is hidden
    from normal ``repr`` output so diagnostics do not expose it.
    """

    otp_verified: bool = False
    verified_caller_ref: str = field(default="", repr=False)

    @property
    def has_verified_caller(self) -> bool:
        return bool(
            self.otp_verified is True
            and isinstance(self.verified_caller_ref, str)
            and self.verified_caller_ref.strip()
        )


@dataclass(frozen=True)
class AuthorizedPublicCall:
    """A validated public read request for a ``ClinicCallTools`` adapter."""

    action: PublicCallAction
    appointment_date: Optional[date] = None


@dataclass(frozen=True)
class AuthorizedProtectedCall:
    """A validated protected request bound to an opaque verified caller."""

    action: ProtectedCallAction
    verified_caller_ref: str = field(repr=False)


AuthorizedCall = Union[AuthorizedPublicCall, AuthorizedProtectedCall]


@dataclass(frozen=True)
class ToolAuthorization:
    """A policy decision that contains no caller-provided sensitive values."""

    status: AuthorizationStatus
    summary: str
    call: Optional[AuthorizedCall] = None

    @property
    def allowed(self) -> bool:
        return self.status is AuthorizationStatus.ALLOWED and self.call is not None

    @property
    def requires_otp(self) -> bool:
        return self.status is AuthorizationStatus.OTP_REQUIRED


class ClinicCallTools(Protocol):
    """Narrow adapter contract for clinic data implementations.

    There is intentionally no ``execute(name, **arguments)`` method. An
    integration can receive only a policy-created public or verified call.
    Implementations belong at the Flask/application boundary, not here.
    """

    def public_information(self, call: AuthorizedPublicCall) -> "PublicInformationResult":
        """Return a safe result for a public, read-only clinic question."""

    def protected_information(self, call: AuthorizedProtectedCall) -> "ProtectedInformationResult":
        """Return a safe result for an OTP-authorized patient question."""


_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "patient_name",
        "patient_mobile",
        "mobile",
        "phone_number",
        "patient_phone",
        "otp",
        "otp_code",
        "verification_code",
        "booking_ref",
        "caller_ref",
        "verified_caller_ref",
        "patient_id",
        "appointment_id",
        "report_id",
        "report_link",
        "download_url",
        "patient_note",
        "notes",
        "symptoms",
        "email",
        "address",
    }
)
_INDIAN_MOBILE_RE = re.compile(r"(?<!\d)(?:\+?91[\s-]?)?[6-9]\d{9}(?!\d)")
_SIX_DIGIT_CODE_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")


def _normalise_field_name(name: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name or "").casefold()).strip("_")


def _is_sensitive_field(name: object) -> bool:
    return _normalise_field_name(name) in _SENSITIVE_FIELD_NAMES


def redact_sensitive_text(text: object, sensitive_values: Tuple[object, ...] = ()) -> str:
    """Remove common patient identifiers from user-visible/loggable text.

    Sensitive result-field values are replaced first, so patient names are
    removed when a backend accidentally places them in a sentence. Indian
    mobile-number and six-digit verification-code patterns are masked as a
    second line of defence.
    """

    redacted = str(text or "")
    for value in sensitive_values:
        candidate = str(value or "").strip()
        if candidate:
            redacted = redacted.replace(candidate, "[redacted]")
    redacted = _INDIAN_MOBILE_RE.sub("[redacted]", redacted)
    return _SIX_DIGIT_CODE_RE.sub("[redacted]", redacted)


def _safe_result_fields(
    fields: Mapping[str, SafeFieldValue],
) -> Tuple[Mapping[str, SafeFieldValue], Tuple[object, ...]]:
    """Return immutable public-safe fields and values hidden from summaries."""

    safe_fields = {}
    sensitive_values = []
    for raw_key, raw_value in fields.items():
        key = str(raw_key)
        if _is_sensitive_field(key):
            sensitive_values.append(raw_value)
            safe_fields[key] = "[redacted]"
        elif isinstance(raw_value, str):
            safe_fields[key] = redact_sensitive_text(raw_value)
        elif isinstance(raw_value, (int, float, bool, date)) or raw_value is None:
            safe_fields[key] = raw_value
        else:
            # Never expose a backend object's repr: it may contain internal
            # IDs or patient data. Adapters must provide deliberate scalars.
            safe_fields[key] = "[redacted]"
    return MappingProxyType(safe_fields), tuple(sensitive_values)


@dataclass(frozen=True)
class PublicInformationResult:
    """A public result whose summary and fields are safe to say or log."""

    action: PublicCallAction
    summary: str
    fields: Mapping[str, SafeFieldValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        raw_fields = self.fields if isinstance(self.fields, Mapping) else {}
        safe_fields, sensitive_values = _safe_result_fields(raw_fields)
        object.__setattr__(self, "summary", redact_sensitive_text(self.summary, sensitive_values))
        object.__setattr__(self, "fields", safe_fields)


@dataclass(frozen=True)
class ProtectedInformationResult:
    """A result that may exist only after the policy allowed OTP access.

    Even after verification, summaries and fields cannot contain a caller's
    name, mobile, OTP, report link, or internal reference. A delivery adapter
    may send a secure link through a verified channel without placing it here.
    """

    action: ProtectedCallAction
    summary: str
    fields: Mapping[str, SafeFieldValue] = field(default_factory=dict)
    otp_verified: bool = True

    def __post_init__(self) -> None:
        raw_fields = self.fields if isinstance(self.fields, Mapping) else {}
        safe_fields, sensitive_values = _safe_result_fields(raw_fields)
        object.__setattr__(self, "summary", redact_sensitive_text(self.summary, sensitive_values))
        object.__setattr__(self, "fields", safe_fields)
        if self.otp_verified is not True:
            # Fail closed if an adapter accidentally constructs a protected
            # result before verification.
            object.__setattr__(
                self,
                "summary",
                "Verification is required before patient-specific information can be shared.",
            )
            object.__setattr__(self, "fields", MappingProxyType({}))


class ClinicCallToolPolicy:
    """Allow-list and validate voice-agent tool calls before app access."""

    _PUBLIC_DATE_ACTIONS = frozenset(
        {PublicCallAction.DOCTOR_SCHEDULE, PublicCallAction.APPOINTMENT_AVAILABILITY}
    )

    def authorize(
        self,
        request: CallToolRequest,
        context: Optional[CallAuthorizationContext] = None,
    ) -> ToolAuthorization:
        """Return an executable typed call or a non-sensitive denial.

        Error messages never echo raw tool arguments because an LLM can put a
        caller's number, OTP, or other personal data in those arguments.
        """

        if not isinstance(request, CallToolRequest):
            return self._denied("Invalid clinic tool request.")
        if not isinstance(request.name, str) or not request.name.strip():
            return self._denied("Invalid clinic tool name.")
        if not isinstance(request.arguments, Mapping) or not all(
            isinstance(key, str) for key in request.arguments
        ):
            return self._denied("Clinic tool arguments must be a named object.")

        name = request.name.strip().casefold()
        public_action = self._public_action(name)
        if public_action is not None:
            return self._authorize_public(public_action, request.arguments)

        protected_action = self._protected_action(name)
        if protected_action is not None:
            return self._authorize_protected(protected_action, request.arguments, context)

        return self._denied("That clinic action is not available.")

    @staticmethod
    def _public_action(name: str) -> Optional[PublicCallAction]:
        try:
            return PublicCallAction(name)
        except ValueError:
            return None

    @staticmethod
    def _protected_action(name: str) -> Optional[ProtectedCallAction]:
        try:
            return ProtectedCallAction(name)
        except ValueError:
            return None

    def _authorize_public(
        self,
        action: PublicCallAction,
        arguments: Mapping[str, object],
    ) -> ToolAuthorization:
        allowed_keys = {"appointment_date"} if action in self._PUBLIC_DATE_ACTIONS else set()
        if set(arguments) - allowed_keys:
            return self._denied("This clinic action received unsupported arguments.")

        appointment_date = None
        if action in self._PUBLIC_DATE_ACTIONS:
            if set(arguments) != {"appointment_date"}:
                return self._denied("This clinic action requires one appointment date.")
            appointment_date = self._parse_date(arguments.get("appointment_date"))
            if appointment_date is None:
                return self._denied("The appointment date must be an ISO date.")

        return ToolAuthorization(
            status=AuthorizationStatus.ALLOWED,
            summary="Public clinic information is authorized.",
            call=AuthorizedPublicCall(action=action, appointment_date=appointment_date),
        )

    def _authorize_protected(
        self,
        action: ProtectedCallAction,
        arguments: Mapping[str, object],
        context: Optional[CallAuthorizationContext],
    ) -> ToolAuthorization:
        # Identity and OTP values must never arrive through LLM tool arguments.
        # The verified session adapter binds the caller independently.
        if arguments:
            return self._denied(
                "Patient-specific clinic actions do not accept caller details as arguments."
            )
        if not isinstance(context, CallAuthorizationContext) or not context.has_verified_caller:
            return ToolAuthorization(
                status=AuthorizationStatus.OTP_REQUIRED,
                summary="OTP verification is required before patient-specific information can be shared.",
            )

        return ToolAuthorization(
            status=AuthorizationStatus.ALLOWED,
            summary="Verified patient information is authorized.",
            call=AuthorizedProtectedCall(
                action=action,
                verified_caller_ref=context.verified_caller_ref.strip(),
            ),
        )

    @staticmethod
    def _parse_date(value: object) -> Optional[date]:
        if isinstance(value, datetime):
            return None
        if isinstance(value, date):
            return value
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _denied(summary: str) -> ToolAuthorization:
        return ToolAuthorization(
            status=AuthorizationStatus.DENIED,
            summary=redact_sensitive_text(summary),
        )
