"""Deterministic clinic, doctor and schedule queries for the AI API."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
import json
from zoneinfo import ZoneInfo

try:
    from ..models import (
        db,
        ClinicProfile,
        ClinicLocation,
        Clinician,
        ClinicScheduleRule,
        ClinicScheduleException,
        ClinicKnowledgeEntry,
        ReceptionSchedule,
        ReceptionScheduleOverride,
    )
    from .clinic_schedule import (
        ArrivalWindow,
        ClinicDateException,
        ClinicDateExceptionKind,
        ClinicScheduleResolver,
        ClinicScheduleStatus,
        ClinicWeekdayRule,
    )
except ImportError:  # pragma: no cover
    from models import (
        db,
        ClinicProfile,
        ClinicLocation,
        Clinician,
        ClinicScheduleRule,
        ClinicScheduleException,
        ClinicKnowledgeEntry,
        ReceptionSchedule,
        ReceptionScheduleOverride,
    )
    from services.clinic_schedule import (
        ArrivalWindow,
        ClinicDateException,
        ClinicDateExceptionKind,
        ClinicScheduleResolver,
        ClinicScheduleStatus,
        ClinicWeekdayRule,
    )


def _json_list(raw_value):
    try:
        value = json.loads(raw_value or "[]")
    except (TypeError, ValueError):
        value = []
    return [str(item).strip() for item in value if str(item).strip()] if isinstance(value, list) else []


def _clock(raw_value):
    try:
        return datetime.strptime(str(raw_value or ""), "%H:%M").time()
    except (TypeError, ValueError):
        return None


def _money(value):
    if value is None:
        return None
    return format(Decimal(value), ".2f")


def active_profile():
    return ClinicProfile.query.filter_by(is_active=True).order_by(ClinicProfile.id.asc()).first()


def clinic_timezone(profile=None, fallback="Asia/Kolkata"):
    timezone_name = str(getattr(profile or active_profile(), "timezone_name", "") or fallback)
    try:
        return ZoneInfo(timezone_name), timezone_name
    except Exception:
        return ZoneInfo(fallback), fallback


def location_accepts_appointments(location):
    """Hospital locations are published for timings/directions, not booking."""
    return str(getattr(location, "location_type", "CLINIC") or "CLINIC").upper() != "HOSPITAL"


def serialize_location(location):
    accepts_appointments = location_accepts_appointments(location)
    return {
        "id": location.id,
        "code": location.code,
        "name": location.display_name,
        "type": location.location_type or "CLINIC",
        "address": location.public_address,
        "landmark": location.landmark,
        "city": location.city,
        "state": location.state,
        "postal_code": location.postal_code,
        "maps_url": location.maps_url,
        "parking_information": location.parking_information,
        "contact_number": location.public_phone,
        "is_default": bool(location.is_default),
        "appointment_booking_enabled": accepts_appointments,
        "booking_mode": "APPOINTMENTS" if accepts_appointments else "INFORMATION_ONLY",
    }


def serialize_clinician(clinician):
    location_ids = {
        row.location_id
        for row in ClinicScheduleRule.query.filter_by(
            clinician_id=clinician.id,
            is_active=True,
        ).all()
    }
    if clinician.default_location_id:
        location_ids.add(clinician.default_location_id)
    return {
        "id": clinician.id,
        "code": clinician.code,
        "name": clinician.display_name,
        "title": clinician.public_title,
        "specialty": clinician.specialty,
        "qualification": clinician.qualification,
        "consultation_fee": _money(clinician.consultation_fee),
        "follow_up_fee": _money(clinician.follow_up_fee),
        "follow_up_days": clinician.follow_up_days,
        "public_bio": clinician.public_bio,
        "sub_specialties": _json_list(clinician.sub_specialties_json),
        "languages": _json_list(clinician.languages_json),
        "conditions_treated": _json_list(clinician.conditions_treated_json),
        "services_offered": _json_list(clinician.services_offered_json),
        "primary_location_id": clinician.default_location_id,
        "location_ids": sorted(location_ids),
    }


def clinic_information():
    profile = active_profile()
    locations = ClinicLocation.query.filter_by(is_active=True).order_by(
        ClinicLocation.is_default.desc(), ClinicLocation.display_name.asc()
    ).all()
    return {
        "configured": bool(profile),
        "clinic": {
            "name": profile.clinic_name if profile else None,
            "phone": profile.phone if profile else None,
            "email": profile.email if profile else None,
            "reception_phone": profile.reception_phone if profile else None,
            "website_url": profile.website_url if profile else None,
            "emergency_wording": profile.emergency_wording if profile else None,
            "consultation_information": profile.consultation_information if profile else None,
            "general_policies": profile.general_policies if profile else None,
            "payment_methods": _json_list(profile.payment_methods_json) if profile else [],
            "available_services": _json_list(profile.available_services_json) if profile else [],
            "timezone": clinic_timezone(profile)[1],
            "appointment_policy": {
                "late_grace_minutes": profile.late_grace_minutes if profile else 15,
                "auto_reschedule_after_minutes": (
                    profile.auto_reschedule_after_minutes if profile else None
                ),
                "late_staff_notification_required": bool(
                    profile.late_staff_notification_required if profile else True
                ),
            },
        },
        "locations": [serialize_location(location) for location in locations],
    }


def _rule_to_domain(rule):
    starts_at = _clock(rule.arrival_window_start)
    ends_at = _clock(rule.arrival_window_end)
    capacity = int(rule.normal_daily_limit or 0) + int(rule.priority_daily_limit or 0)
    return ClinicWeekdayRule(
        weekday=int(rule.weekday),
        enabled=bool(rule.booking_enabled and rule.is_active),
        arrival_window=ArrivalWindow(starts_at, ends_at) if starts_at and ends_at else None,
        daily_capacity=capacity,
        individual_time_slots=bool(rule.individual_time_slots),
    )


def _exception_to_domain(row):
    exception_type = str(row.exception_type or "").upper()
    starts_at = _clock(row.arrival_window_start)
    ends_at = _clock(row.arrival_window_end)
    capacity = int(row.normal_daily_limit or 0) + int(row.priority_daily_limit or 0)
    return ClinicDateException(
        appointment_date=row.schedule_date,
        kind=ClinicDateExceptionKind(exception_type),
        enabled=bool(row.is_active),
        arrival_window=(
            ArrivalWindow(starts_at, ends_at)
            if exception_type == "OVERRIDE" and starts_at and ends_at
            else None
        ),
        daily_capacity=capacity if exception_type == "OVERRIDE" else None,
    )


def _matching_rules(location_id, clinician_id, target_date):
    query = ClinicScheduleRule.query.filter_by(
        location_id=location_id,
        clinician_id=clinician_id,
        is_active=True,
    ).filter(ClinicScheduleRule.weekday == target_date.weekday())
    rows = query.order_by(ClinicScheduleRule.effective_from.desc(), ClinicScheduleRule.id.desc()).all()
    return [
        row
        for row in rows
        if (row.effective_from is None or row.effective_from <= target_date)
        and (row.effective_to is None or row.effective_to >= target_date)
    ][:1]


def _matching_exceptions(location_id, clinician_id, target_date):
    return ClinicScheduleException.query.filter_by(
        location_id=location_id,
        clinician_id=clinician_id,
        schedule_date=target_date,
        is_active=True,
    ).order_by(ClinicScheduleException.id.desc()).all()


def resolve_schedule(location_id, target_date, clinician_id=None):
    rules = _matching_rules(location_id, clinician_id, target_date)
    exceptions = _matching_exceptions(location_id, clinician_id, target_date)
    resolver = ClinicScheduleResolver(
        weekday_rules=[_rule_to_domain(row) for row in rules],
        date_exceptions=[_exception_to_domain(row) for row in exceptions],
    )
    return resolver.resolve(target_date), (rules[0] if rules else None), exceptions


def doctor_schedule_resolution(location_id, clinician_id, target_date):
    clinic_result, clinic_rule, clinic_exceptions = resolve_schedule(location_id, target_date, None)
    # A clinic-wide rule is an optional location-level gate. Most clinics only
    # configure each doctor's actual location timing, so the absence of a
    # clinic-wide row must not make a valid doctor schedule unavailable.
    if (clinic_rule or clinic_exceptions) and clinic_result.status is not ClinicScheduleStatus.OPEN:
        return clinic_result, None, clinic_exceptions
    doctor_result, doctor_rule, doctor_exceptions = resolve_schedule(
        location_id, target_date, clinician_id
    )
    return doctor_result, doctor_rule, doctor_exceptions


def schedule_payload(result, target_date, *, rule=None, location_id=None, clinician_id=None):
    public = result.to_public()
    location = db.session.get(ClinicLocation, location_id) if location_id else None
    accepts_appointments = location_accepts_appointments(location) if location else True
    message = public.message
    if not accepts_appointments and result.status is ClinicScheduleStatus.OPEN:
        message = "Doctor timing is available here, but appointments must be booked at the clinic."
    return {
        "date": target_date.isoformat(),
        "status": public.status.value,
        "bookable": bool(public.is_bookable and accepts_appointments),
        "arrival_window": public.arrival_window_label,
        "effective_start": (
            result.arrival_window.starts_at.isoformat(timespec="minutes")
            if result.arrival_window
            else None
        ),
        "effective_end": (
            result.arrival_window.ends_at.isoformat(timespec="minutes")
            if result.arrival_window
            else None
        ),
        "daily_capacity": public.daily_capacity,
        "message": message,
        "location_id": location_id,
        "doctor_id": clinician_id,
        "individual_time_slots": bool(public.is_individual_time_slot),
        "slot_duration_minutes": int(rule.slot_duration_minutes or 20) if rule else None,
        "max_patients_per_slot": int(rule.max_patients_per_slot or 1) if rule else None,
        "appointment_booking_enabled": accepts_appointments,
    }


def clinic_live_status(location, *, at=None):
    timezone_value, timezone_name = clinic_timezone()
    current = at.astimezone(timezone_value) if at and at.tzinfo else (at or datetime.now(timezone_value))
    result, rule, _exceptions = resolve_schedule(location.id, current.date(), None)
    payload = schedule_payload(result, current.date(), rule=rule, location_id=location.id)
    payload["timezone"] = timezone_name
    payload["checked_at"] = current.isoformat()
    payload["currently_open"] = False
    if result.status is ClinicScheduleStatus.OPEN and result.arrival_window:
        payload["currently_open"] = result.arrival_window.starts_at <= current.time().replace(tzinfo=None) < result.arrival_window.ends_at
    return payload


def find_next_available_date(location_id, *, clinician_id=None, start_date=None, days=90):
    start = start_date or datetime.now(clinic_timezone()[0]).date()
    for offset in range(max(1, min(int(days), 180))):
        candidate = start + timedelta(days=offset)
        if clinician_id:
            result, rule, _ = doctor_schedule_resolution(location_id, clinician_id, candidate)
        else:
            result, rule, _ = resolve_schedule(location_id, candidate, None)
        if result.is_bookable:
            return schedule_payload(
                result,
                candidate,
                rule=rule,
                location_id=location_id,
                clinician_id=clinician_id,
            )
    return None


def reception_status(location, *, at=None):
    timezone_value, timezone_name = clinic_timezone()
    current = at.astimezone(timezone_value) if at and at.tzinfo else (at or datetime.now(timezone_value))
    override = ReceptionScheduleOverride.query.filter_by(
        location_id=location.id,
        schedule_date=current.date(),
        is_active=True,
    ).order_by(ReceptionScheduleOverride.id.desc()).first()
    schedule = override or ReceptionSchedule.query.filter_by(
        location_id=location.id,
        weekday=current.weekday(),
        is_active=True,
    ).order_by(ReceptionSchedule.id.desc()).first()
    is_open_day = bool(schedule and schedule.is_open)
    currently_open = bool(
        is_open_day
        and schedule.open_time
        and schedule.close_time
        and schedule.open_time <= current.time().replace(tzinfo=None) < schedule.close_time
    )
    next_available_at = None
    search_start = current.date()
    for offset in range(15):
        candidate_date = search_start + timedelta(days=offset)
        candidate = ReceptionScheduleOverride.query.filter_by(
            location_id=location.id,
            schedule_date=candidate_date,
            is_active=True,
        ).order_by(ReceptionScheduleOverride.id.desc()).first()
        if candidate is None:
            candidate = ReceptionSchedule.query.filter_by(
                location_id=location.id,
                weekday=candidate_date.weekday(),
                is_active=True,
            ).order_by(ReceptionSchedule.id.desc()).first()
        if not candidate or not candidate.is_open or not candidate.open_time or not candidate.close_time:
            continue
        candidate_open = datetime.combine(
            candidate_date, candidate.open_time, tzinfo=timezone_value
        )
        candidate_close = datetime.combine(
            candidate_date, candidate.close_time, tzinfo=timezone_value
        )
        if current < candidate_open:
            next_available_at = candidate_open.isoformat()
            break
        if candidate_open <= current < candidate_close:
            next_available_at = current.isoformat()
            break
    profile = active_profile()
    return {
        "location_id": location.id,
        "date": current.date().isoformat(),
        "timezone": timezone_name,
        "status": "OPEN" if currently_open else "CLOSED",
        "scheduled_open": is_open_day,
        "opens_at": schedule.open_time.isoformat(timespec="minutes") if schedule and schedule.open_time else None,
        "closes_at": schedule.close_time.isoformat(timespec="minutes") if schedule and schedule.close_time else None,
        "public_note": schedule.public_note if schedule else None,
        "next_available_at": next_available_at,
        "transfer_destination": profile.reception_phone if profile else None,
    }


def knowledge_entries(*, category=None, language=None, limit=50, offset=0):
    query = ClinicKnowledgeEntry.query.filter_by(is_active=True)
    if category:
        query = query.filter(ClinicKnowledgeEntry.category == category)
    if language:
        query = query.filter(ClinicKnowledgeEntry.language == language)
    rows = query.order_by(
        ClinicKnowledgeEntry.category.asc(), ClinicKnowledgeEntry.id.asc()
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 100))).all()
    return [
        {
            "id": row.id,
            "category": row.category,
            "question": row.question,
            "answer": row.answer,
            "language": row.language,
            "keywords": _json_list(row.keywords_json),
        }
        for row in rows
    ]
