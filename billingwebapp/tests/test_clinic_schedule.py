import unittest
from datetime import date, datetime, time

from services.clinic_schedule import (
    AppointmentDateRange,
    ArrivalWindow,
    ClinicDateException,
    ClinicDateExceptionKind,
    ClinicScheduleResolver,
    ClinicScheduleStatus,
    ClinicWeekdayRule,
    ScheduleConfigurationError,
    ScheduleDateError,
)


MONDAY = date(2026, 8, 24)
TUESDAY = date(2026, 8, 25)
WEDNESDAY = date(2026, 8, 26)


def standard_window():
    return ArrivalWindow(time(17, 30), time(19, 45))


class ClinicScheduleResolverTests(unittest.TestCase):
    def test_enabled_weekday_rule_returns_fcfs_public_availability(self):
        resolver = ClinicScheduleResolver(
            weekday_rules=[
                ClinicWeekdayRule(
                    weekday=0,
                    arrival_window=standard_window(),
                    daily_capacity=15,
                )
            ]
        )

        resolved = resolver.resolve(MONDAY)
        public = resolved.to_public()

        self.assertEqual(resolved.status, ClinicScheduleStatus.OPEN)
        self.assertEqual(public.arrival_window_label, "5:30 PM to 7:45 PM")
        self.assertEqual(public.daily_capacity, 15)
        self.assertTrue(public.is_bookable)
        self.assertFalse(public.is_individual_time_slot)
        self.assertNotIn("slot", public.message.casefold())

    def test_individual_time_slots_flag_is_carried_through_to_public_output(self):
        resolver = ClinicScheduleResolver(
            weekday_rules=[
                ClinicWeekdayRule(
                    weekday=0,
                    arrival_window=standard_window(),
                    daily_capacity=15,
                    individual_time_slots=True,
                )
            ]
        )

        public = resolver.resolve(MONDAY).to_public()

        self.assertTrue(public.is_individual_time_slot)

    def test_no_enabled_matching_rule_is_unconfigured_not_open(self):
        resolver = ClinicScheduleResolver(
            weekday_rules=[
                ClinicWeekdayRule(
                    weekday=0,
                    enabled=False,
                    arrival_window=standard_window(),
                    daily_capacity=15,
                )
            ]
        )

        resolved = resolver.resolve(MONDAY)

        self.assertEqual(resolved.status, ClinicScheduleStatus.UNCONFIGURED)
        self.assertIsNone(resolved.arrival_window)
        self.assertIsNone(resolved.daily_capacity)
        self.assertFalse(resolved.to_public().is_bookable)

    def test_override_can_open_a_date_without_a_weekday_rule(self):
        override_window = ArrivalWindow(time(9, 0), time(11, 30))
        resolver = ClinicScheduleResolver(
            date_exceptions=[
                ClinicDateException(
                    appointment_date=TUESDAY,
                    kind=ClinicDateExceptionKind.OVERRIDE,
                    arrival_window=override_window,
                    daily_capacity=6,
                )
            ]
        )

        resolved = resolver.resolve(TUESDAY)

        self.assertEqual(resolved.status, ClinicScheduleStatus.OPEN)
        self.assertEqual(resolved.arrival_window, override_window)
        self.assertEqual(resolved.daily_capacity, 6)

    def test_date_exception_precedence_is_closed_holiday_leave_then_override(self):
        resolver = ClinicScheduleResolver(
            weekday_rules=[
                ClinicWeekdayRule(
                    weekday=0,
                    arrival_window=standard_window(),
                    daily_capacity=15,
                )
            ],
            date_exceptions=[
                ClinicDateException(
                    appointment_date=MONDAY,
                    kind=ClinicDateExceptionKind.OVERRIDE,
                    arrival_window=ArrivalWindow(time(9, 0), time(11, 0)),
                    daily_capacity=5,
                ),
                ClinicDateException(
                    appointment_date=MONDAY,
                    kind=ClinicDateExceptionKind.LEAVE,
                    private_reason="Dr Riya is attending a confidential appointment.",
                ),
                ClinicDateException(
                    appointment_date=MONDAY,
                    kind=ClinicDateExceptionKind.HOLIDAY,
                ),
                ClinicDateException(
                    appointment_date=MONDAY,
                    kind=ClinicDateExceptionKind.CLOSED,
                ),
            ],
        )

        resolved = resolver.resolve(MONDAY)

        self.assertEqual(resolved.status, ClinicScheduleStatus.CLOSED)
        self.assertIsNone(resolved.arrival_window)
        self.assertIsNone(resolved.daily_capacity)

    def test_disabled_closure_does_not_block_enabled_override(self):
        override_window = ArrivalWindow(time(10, 0), time(12, 0))
        resolver = ClinicScheduleResolver(
            date_exceptions=[
                ClinicDateException(
                    appointment_date=TUESDAY,
                    kind=ClinicDateExceptionKind.CLOSED,
                    enabled=False,
                ),
                ClinicDateException(
                    appointment_date=TUESDAY,
                    kind=ClinicDateExceptionKind.OVERRIDE,
                    arrival_window=override_window,
                    daily_capacity=4,
                ),
            ]
        )

        resolved = resolver.resolve(TUESDAY)

        self.assertEqual(resolved.status, ClinicScheduleStatus.OPEN)
        self.assertEqual(resolved.arrival_window, override_window)

    def test_leave_reason_never_appears_in_resolved_or_public_output(self):
        private_reason = "Doctor Meera is on medical leave for a private reason."
        leave = ClinicDateException(
            appointment_date=MONDAY,
            kind=ClinicDateExceptionKind.LEAVE,
            private_reason=private_reason,
        )
        resolver = ClinicScheduleResolver(date_exceptions=[leave])

        resolved = resolver.resolve(MONDAY)
        public = resolved.to_public()

        self.assertNotIn(private_reason, repr(leave))
        self.assertNotIn(private_reason, repr(resolved))
        self.assertNotIn(private_reason, repr(public))
        self.assertNotIn(private_reason, public.message)
        self.assertEqual(public.message, "Online appointments are not available for this date.")

    def test_booking_range_is_inclusive_and_cannot_be_bypassed_by_override(self):
        resolver = ClinicScheduleResolver(
            date_exceptions=[
                ClinicDateException(
                    appointment_date=WEDNESDAY,
                    kind=ClinicDateExceptionKind.OVERRIDE,
                    arrival_window=standard_window(),
                    daily_capacity=10,
                )
            ],
            booking_date_range=AppointmentDateRange(MONDAY, TUESDAY),
        )

        self.assertEqual(
            resolver.resolve(WEDNESDAY).status, ClinicScheduleStatus.CLOSED
        )

    def test_zero_capacity_is_open_schedule_but_not_bookable(self):
        resolver = ClinicScheduleResolver(
            weekday_rules=[
                ClinicWeekdayRule(
                    weekday=0,
                    arrival_window=standard_window(),
                    daily_capacity=0,
                )
            ]
        )

        public = resolver.resolve(MONDAY).to_public()

        self.assertEqual(public.status, ClinicScheduleStatus.OPEN)
        self.assertFalse(public.is_bookable)
        self.assertIn("no online appointments", public.message.casefold())


class ClinicScheduleValidationTests(unittest.TestCase):
    def test_invalid_date_range_is_rejected(self):
        with self.assertRaises(ScheduleConfigurationError):
            AppointmentDateRange(TUESDAY, MONDAY)

    def test_resolver_rejects_string_and_datetime_dates(self):
        resolver = ClinicScheduleResolver()
        for invalid in ("2026-08-24", datetime(2026, 8, 24, 9, 0), None):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ScheduleDateError):
                    resolver.resolve(invalid)

    def test_enabled_rule_and_override_require_complete_availability(self):
        with self.assertRaises(ScheduleConfigurationError):
            ClinicWeekdayRule(weekday=0, daily_capacity=5)
        with self.assertRaises(ScheduleConfigurationError):
            ClinicDateException(
                appointment_date=MONDAY,
                kind=ClinicDateExceptionKind.OVERRIDE,
                arrival_window=standard_window(),
            )

    def test_invalid_window_capacity_and_closure_availability_are_rejected(self):
        with self.assertRaises(ScheduleConfigurationError):
            ArrivalWindow(time(17, 30), time(17, 30))
        with self.assertRaises(ScheduleConfigurationError):
            ClinicWeekdayRule(
                weekday=0,
                arrival_window=standard_window(),
                daily_capacity=-1,
            )
        with self.assertRaises(ScheduleConfigurationError):
            ClinicDateException(
                appointment_date=MONDAY,
                kind=ClinicDateExceptionKind.HOLIDAY,
                daily_capacity=10,
            )

    def test_ambiguous_enabled_rules_and_same_precedence_exceptions_are_rejected(self):
        rule = ClinicWeekdayRule(
            weekday=0,
            arrival_window=standard_window(),
            daily_capacity=10,
        )
        with self.assertRaises(ScheduleConfigurationError):
            ClinicScheduleResolver(weekday_rules=[rule, rule])

        with self.assertRaises(ScheduleConfigurationError):
            ClinicScheduleResolver(
                date_exceptions=[
                    ClinicDateException(MONDAY, ClinicDateExceptionKind.HOLIDAY),
                    ClinicDateException(MONDAY, ClinicDateExceptionKind.HOLIDAY),
                ]
            )


if __name__ == "__main__":
    unittest.main()
