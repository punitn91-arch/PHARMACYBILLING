"""Deterministic, public-safe clinic schedule resolution.

This module is deliberately independent of Flask, SQLAlchemy, telephony, and
patient records.  It is the configuration boundary a call/public-booking
adapter can use before it asks the existing appointment service for live
remaining capacity.

The resolver models an FCFS clinic *arrival window*, never an individual
appointment time slot.  A missing enabled weekday rule is intentionally
``UNCONFIGURED`` instead of inventing availability from old defaults.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from enum import Enum
from typing import Iterable, Optional, Tuple


class ScheduleConfigurationError(ValueError):
    """Raised when a schedule configuration is ambiguous or unsafe."""


class ScheduleDateError(ValueError):
    """Raised when a resolver query is not a calendar ``date``."""


class ClinicScheduleStatus(str, Enum):
    """The only public scheduling states exposed by this resolver."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    UNCONFIGURED = "UNCONFIGURED"


class ClinicDateExceptionKind(str, Enum):
    """Supported date-level exceptions, in their safe resolution order."""

    CLOSED = "CLOSED"
    HOLIDAY = "HOLIDAY"
    LEAVE = "LEAVE"
    OVERRIDE = "OVERRIDE"


_EXCEPTION_PRECEDENCE = {
    ClinicDateExceptionKind.CLOSED: 4,
    ClinicDateExceptionKind.HOLIDAY: 3,
    ClinicDateExceptionKind.LEAVE: 2,
    ClinicDateExceptionKind.OVERRIDE: 1,
}
_CLOSING_EXCEPTIONS = frozenset(
    {
        ClinicDateExceptionKind.CLOSED,
        ClinicDateExceptionKind.HOLIDAY,
        ClinicDateExceptionKind.LEAVE,
    }
)


def _require_date(value: object, *, field_name: str) -> date:
    """Accept a plain date, never an accidental datetime or string."""

    if isinstance(value, datetime) or not isinstance(value, date):
        raise ScheduleDateError(f"{field_name} must be a calendar date.")
    return value


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ScheduleConfigurationError(f"{field_name} must be true or false.")
    return value


def _require_capacity(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScheduleConfigurationError(
            f"{field_name} must be a whole number greater than or equal to zero."
        )
    return value


def _coerce_exception_kind(value: object) -> ClinicDateExceptionKind:
    if isinstance(value, ClinicDateExceptionKind):
        return value
    if isinstance(value, str):
        try:
            return ClinicDateExceptionKind(value.strip().upper())
        except ValueError:
            pass
    raise ScheduleConfigurationError(
        "exception kind must be CLOSED, HOLIDAY, LEAVE, or OVERRIDE."
    )


@dataclass(frozen=True)
class ArrivalWindow:
    """A same-day FCFS arrival interval that is safe to tell a caller."""

    starts_at: time
    ends_at: time

    def __post_init__(self) -> None:
        if not isinstance(self.starts_at, time) or not isinstance(self.ends_at, time):
            raise ScheduleConfigurationError("arrival window times must be time values.")
        if self.starts_at.tzinfo is not None or self.ends_at.tzinfo is not None:
            raise ScheduleConfigurationError("arrival window times must not include a timezone.")
        if self.starts_at >= self.ends_at:
            raise ScheduleConfigurationError(
                "arrival window must end after it starts on the same day."
            )

    @staticmethod
    def _format_clock(value: time) -> str:
        hour = value.hour % 12 or 12
        suffix = "AM" if value.hour < 12 else "PM"
        if value.minute:
            return f"{hour}:{value.minute:02d} {suffix}"
        return f"{hour} {suffix}"

    @property
    def label(self) -> str:
        """Human-readable arrival window, not an individual appointment slot."""

        return f"{self._format_clock(self.starts_at)} to {self._format_clock(self.ends_at)}"


@dataclass(frozen=True)
class AppointmentDateRange:
    """Optional inclusive range in which public/voice booking may be offered."""

    starts_on: date
    ends_on: date

    def __post_init__(self) -> None:
        starts_on = _require_date(self.starts_on, field_name="starts_on")
        ends_on = _require_date(self.ends_on, field_name="ends_on")
        if starts_on > ends_on:
            raise ScheduleConfigurationError(
                "appointment date range must start on or before it ends."
            )

    def contains(self, appointment_date: date) -> bool:
        appointment_date = _require_date(
            appointment_date, field_name="appointment_date"
        )
        return self.starts_on <= appointment_date <= self.ends_on


@dataclass(frozen=True)
class ClinicWeekdayRule:
    """A global clinic schedule rule for Monday=0 through Sunday=6.

    Disabled rules may retain their arrival-window configuration for future
    re-enablement, but they do not produce availability.  An enabled rule
    must always have a window and an explicit maximum FCFS capacity.
    """

    weekday: int
    enabled: bool = True
    arrival_window: Optional[ArrivalWindow] = None
    daily_capacity: Optional[int] = None
    # False (default): a single FCFS arrival-window capacity pool, matching
    # this class's historical contract. True: the window is additionally
    # divisible into individually bookable times (FIXED_SLOT mode).
    individual_time_slots: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.weekday, bool) or not isinstance(self.weekday, int):
            raise ScheduleConfigurationError("weekday must be an integer from 0 to 6.")
        if not 0 <= self.weekday <= 6:
            raise ScheduleConfigurationError("weekday must be an integer from 0 to 6.")
        _require_bool(self.enabled, field_name="enabled")
        _require_bool(self.individual_time_slots, field_name="individual_time_slots")
        if self.arrival_window is not None and not isinstance(
            self.arrival_window, ArrivalWindow
        ):
            raise ScheduleConfigurationError("arrival_window must be an ArrivalWindow.")
        if self.daily_capacity is not None:
            _require_capacity(self.daily_capacity, field_name="daily_capacity")
        if self.enabled and (
            self.arrival_window is None or self.daily_capacity is None
        ):
            raise ScheduleConfigurationError(
                "an enabled weekday rule requires an arrival window and daily capacity."
            )


@dataclass(frozen=True)
class ClinicDateException:
    """A date-specific closure or schedule override.

    ``private_reason`` is deliberately hidden from ``repr`` and is never
    returned by the resolver.  In particular, leave reasons must not appear
    in public voice replies, logs, or booking output.
    """

    appointment_date: date
    kind: ClinicDateExceptionKind
    enabled: bool = True
    arrival_window: Optional[ArrivalWindow] = None
    daily_capacity: Optional[int] = None
    private_reason: str = field(default="", repr=False, compare=False)

    def __post_init__(self) -> None:
        _require_date(self.appointment_date, field_name="appointment_date")
        kind = _coerce_exception_kind(self.kind)
        object.__setattr__(self, "kind", kind)
        _require_bool(self.enabled, field_name="enabled")
        if not isinstance(self.private_reason, str):
            raise ScheduleConfigurationError("private_reason must be text when supplied.")
        if self.arrival_window is not None and not isinstance(
            self.arrival_window, ArrivalWindow
        ):
            raise ScheduleConfigurationError("arrival_window must be an ArrivalWindow.")
        if self.daily_capacity is not None:
            _require_capacity(self.daily_capacity, field_name="daily_capacity")

        if kind is ClinicDateExceptionKind.OVERRIDE:
            if self.arrival_window is None or self.daily_capacity is None:
                raise ScheduleConfigurationError(
                    "an OVERRIDE exception requires an arrival window and daily capacity."
                )
        elif self.arrival_window is not None or self.daily_capacity is not None:
            raise ScheduleConfigurationError(
                "CLOSED, HOLIDAY, and LEAVE exceptions cannot contain availability."
            )


@dataclass(frozen=True)
class PublicClinicSchedule:
    """The deliberately small schedule result safe for a caller or public UI."""

    appointment_date: date
    status: ClinicScheduleStatus
    arrival_window_label: Optional[str]
    daily_capacity: Optional[int]
    is_individual_time_slot: bool
    message: str

    @property
    def is_bookable(self) -> bool:
        """Whether this configuration can offer any FCFS appointment capacity."""

        return self.status is ClinicScheduleStatus.OPEN and (self.daily_capacity or 0) > 0


@dataclass(frozen=True)
class ClinicScheduleResult:
    """Internal resolution without any private exception reason or record ID."""

    appointment_date: date
    status: ClinicScheduleStatus
    arrival_window: Optional[ArrivalWindow] = None
    daily_capacity: Optional[int] = None
    individual_time_slots: bool = False

    def __post_init__(self) -> None:
        _require_date(self.appointment_date, field_name="appointment_date")
        if not isinstance(self.status, ClinicScheduleStatus):
            raise ScheduleConfigurationError("status must be a ClinicScheduleStatus.")
        _require_bool(self.individual_time_slots, field_name="individual_time_slots")
        if self.status is ClinicScheduleStatus.OPEN:
            if self.arrival_window is None or self.daily_capacity is None:
                raise ScheduleConfigurationError(
                    "an OPEN schedule result requires an arrival window and daily capacity."
                )
            if not isinstance(self.arrival_window, ArrivalWindow):
                raise ScheduleConfigurationError("arrival_window must be an ArrivalWindow.")
            _require_capacity(self.daily_capacity, field_name="daily_capacity")
        elif self.arrival_window is not None or self.daily_capacity is not None:
            raise ScheduleConfigurationError(
                "CLOSED and UNCONFIGURED schedule results cannot contain availability."
            )

    @property
    def is_bookable(self) -> bool:
        return self.status is ClinicScheduleStatus.OPEN and (self.daily_capacity or 0) > 0

    def to_public(self) -> PublicClinicSchedule:
        """Return the only output intended for callers and public booking pages."""

        if self.status is ClinicScheduleStatus.OPEN:
            assert self.arrival_window is not None
            if self.is_bookable:
                message = "Online appointments are available for this date."
            else:
                message = (
                    "The clinic is open, but no online appointments are currently "
                    "available for this date."
                )
            return PublicClinicSchedule(
                appointment_date=self.appointment_date,
                status=self.status,
                arrival_window_label=self.arrival_window.label,
                daily_capacity=self.daily_capacity,
                is_individual_time_slot=self.individual_time_slots,
                message=message,
            )

        if self.status is ClinicScheduleStatus.CLOSED:
            message = "Online appointments are not available for this date."
        else:
            message = "Online appointment scheduling is not configured for this date."
        return PublicClinicSchedule(
            appointment_date=self.appointment_date,
            status=self.status,
            arrival_window_label=None,
            daily_capacity=None,
            is_individual_time_slot=False,
            message=message,
        )


class ClinicScheduleResolver:
    """Resolve clinic-wide FCFS availability from explicit schedule configuration.

    Date exception precedence is fixed and does not depend on database order:
    ``CLOSED`` > ``HOLIDAY`` > ``LEAVE`` > ``OVERRIDE``.  A configured booking
    date range is enforced before exceptions, so an override cannot silently
    make an out-of-range date bookable.
    """

    def __init__(
        self,
        weekday_rules: Iterable[ClinicWeekdayRule] = (),
        date_exceptions: Iterable[ClinicDateException] = (),
        *,
        booking_date_range: Optional[AppointmentDateRange] = None,
    ) -> None:
        if booking_date_range is not None and not isinstance(
            booking_date_range, AppointmentDateRange
        ):
            raise ScheduleConfigurationError(
                "booking_date_range must be an AppointmentDateRange when supplied."
            )
        self.booking_date_range = booking_date_range
        self._weekday_rules = self._validate_weekday_rules(weekday_rules)
        self._date_exceptions = self._validate_date_exceptions(date_exceptions)

    @staticmethod
    def _validate_weekday_rules(
        rules: Iterable[ClinicWeekdayRule],
    ) -> Tuple[ClinicWeekdayRule, ...]:
        try:
            copied_rules = tuple(rules)
        except TypeError as exc:
            raise ScheduleConfigurationError("weekday_rules must be iterable.") from exc

        enabled_weekdays = set()
        for rule in copied_rules:
            if not isinstance(rule, ClinicWeekdayRule):
                raise ScheduleConfigurationError(
                    "weekday_rules must contain ClinicWeekdayRule values."
                )
            if rule.enabled:
                if rule.weekday in enabled_weekdays:
                    raise ScheduleConfigurationError(
                        "only one enabled weekday rule is allowed for each weekday."
                    )
                enabled_weekdays.add(rule.weekday)
        return copied_rules

    @staticmethod
    def _validate_date_exceptions(
        exceptions: Iterable[ClinicDateException],
    ) -> Tuple[ClinicDateException, ...]:
        try:
            copied_exceptions = tuple(exceptions)
        except TypeError as exc:
            raise ScheduleConfigurationError("date_exceptions must be iterable.") from exc

        seen_priorities = set()
        for exception in copied_exceptions:
            if not isinstance(exception, ClinicDateException):
                raise ScheduleConfigurationError(
                    "date_exceptions must contain ClinicDateException values."
                )
            if exception.enabled:
                key = (
                    exception.appointment_date,
                    _EXCEPTION_PRECEDENCE[exception.kind],
                )
                if key in seen_priorities:
                    raise ScheduleConfigurationError(
                        "only one enabled exception is allowed for a date at each precedence."
                    )
                seen_priorities.add(key)
        return copied_exceptions

    def _best_exception_for(self, appointment_date: date) -> Optional[ClinicDateException]:
        applicable = (
            exception
            for exception in self._date_exceptions
            if exception.enabled and exception.appointment_date == appointment_date
        )
        return max(
            applicable,
            key=lambda exception: _EXCEPTION_PRECEDENCE[exception.kind],
            default=None,
        )

    def _enabled_rule_for(self, appointment_date: date) -> Optional[ClinicWeekdayRule]:
        return next(
            (
                rule
                for rule in self._weekday_rules
                if rule.enabled and rule.weekday == appointment_date.weekday()
            ),
            None,
        )

    def resolve(self, appointment_date: date) -> ClinicScheduleResult:
        """Resolve one date without consulting patient data or booking records."""

        appointment_date = _require_date(
            appointment_date, field_name="appointment_date"
        )
        if self.booking_date_range and not self.booking_date_range.contains(appointment_date):
            return ClinicScheduleResult(
                appointment_date=appointment_date,
                status=ClinicScheduleStatus.CLOSED,
            )

        exception = self._best_exception_for(appointment_date)
        if exception is not None:
            if exception.kind in _CLOSING_EXCEPTIONS:
                return ClinicScheduleResult(
                    appointment_date=appointment_date,
                    status=ClinicScheduleStatus.CLOSED,
                )
            # ``ClinicDateException`` validates this pair for an OVERRIDE.
            # An override date always falls back to the plain FCFS pool: it
            # has no per-exception fixed-slot configuration of its own.
            return ClinicScheduleResult(
                appointment_date=appointment_date,
                status=ClinicScheduleStatus.OPEN,
                arrival_window=exception.arrival_window,
                daily_capacity=exception.daily_capacity,
                individual_time_slots=False,
            )

        rule = self._enabled_rule_for(appointment_date)
        if rule is None:
            # Preserve current FCFS behaviour safely: no rule never means an
            # invented slot or capacity.
            return ClinicScheduleResult(
                appointment_date=appointment_date,
                status=ClinicScheduleStatus.UNCONFIGURED,
            )

        return ClinicScheduleResult(
            appointment_date=appointment_date,
            status=ClinicScheduleStatus.OPEN,
            arrival_window=rule.arrival_window,
            daily_capacity=rule.daily_capacity,
            individual_time_slots=rule.individual_time_slots,
        )


__all__ = [
    "AppointmentDateRange",
    "ArrivalWindow",
    "ClinicDateException",
    "ClinicDateExceptionKind",
    "ClinicScheduleResolver",
    "ClinicScheduleResult",
    "ClinicScheduleStatus",
    "ClinicWeekdayRule",
    "PublicClinicSchedule",
    "ScheduleConfigurationError",
    "ScheduleDateError",
]
