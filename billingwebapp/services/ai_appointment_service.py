"""Deterministic slot and transactional appointment operations for AI calls."""

from datetime import date, datetime, time, timedelta
import hashlib
import json
import re
import secrets

from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError

try:
    from ..models import (
        db,
        Appointment,
        AppointmentSlotBlock,
        AppointmentSlotLock,
        AppointmentWaitlist,
        AIIdempotencyRecord,
        ClinicLocation,
        Clinician,
        Patient,
    )
    from .ai_clinic_service import (
        clinic_timezone,
        doctor_schedule_resolution,
        location_accepts_appointments,
    )
    from .clinic_schedule import ClinicScheduleStatus
except ImportError:  # pragma: no cover
    from models import (
        db,
        Appointment,
        AppointmentSlotBlock,
        AppointmentSlotLock,
        AppointmentWaitlist,
        AIIdempotencyRecord,
        ClinicLocation,
        Clinician,
        Patient,
    )
    from services.ai_clinic_service import (
        clinic_timezone,
        doctor_schedule_resolution,
        location_accepts_appointments,
    )
    from services.clinic_schedule import ClinicScheduleStatus


ACTIVE_APPOINTMENT_STATUSES = frozenset({"BOOKED", "CONFIRMED", "CHECKED_IN", "LATE"})
_IDEMPOTENCY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")


class AppointmentAPIError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _sha256(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def canonical_fingerprint(payload):
    return _sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))


def begin_idempotency(db_session, *, api_client_id, operation, raw_key, payload, now=None):
    now = now or datetime.utcnow()
    key = str(raw_key or "").strip()
    if not _IDEMPOTENCY_PATTERN.fullmatch(key):
        raise AppointmentAPIError(
            "INVALID_REQUEST",
            "Idempotency-Key must be 8-128 safe identifier characters",
            400,
        )
    key_hash = _sha256(key)
    fingerprint = canonical_fingerprint(payload)
    existing = AIIdempotencyRecord.query.filter_by(
        api_client_id=api_client_id,
        operation=operation,
        idempotency_key_hash=key_hash,
    ).first()
    if existing:
        if existing.request_fingerprint != fingerprint:
            raise AppointmentAPIError(
                "IDEMPOTENCY_CONFLICT",
                "Idempotency key was already used with a different request",
                409,
            )
        if existing.state == "COMPLETED" and existing.response_json:
            return existing, json.loads(existing.response_json), int(existing.response_status or 200)
        raise AppointmentAPIError("REQUEST_IN_PROGRESS", "The original request is still processing", 409)
    record = AIIdempotencyRecord(
        api_client_id=api_client_id,
        operation=operation,
        idempotency_key_hash=key_hash,
        request_fingerprint=fingerprint,
        state="IN_PROGRESS",
        expires_at=now + timedelta(hours=24),
        created_at=now,
        updated_at=now,
    )
    try:
        with db_session.begin_nested():
            db_session.add(record)
            db_session.flush()
    except IntegrityError:
        existing = AIIdempotencyRecord.query.filter_by(
            api_client_id=api_client_id,
            operation=operation,
            idempotency_key_hash=key_hash,
        ).first()
        if existing and existing.request_fingerprint == fingerprint:
            if existing.state == "COMPLETED" and existing.response_json:
                return existing, json.loads(existing.response_json), int(existing.response_status or 200)
            raise AppointmentAPIError("REQUEST_IN_PROGRESS", "The original request is still processing", 409)
        if existing:
            raise AppointmentAPIError(
                "IDEMPOTENCY_CONFLICT",
                "Idempotency key was already used with a different request",
                409,
            )
        raise AppointmentAPIError("REQUEST_IN_PROGRESS", "The original request is still processing", 409)
    return record, None, None


def complete_idempotency(record, response_data, status_code, *, resource_type=None, resource_id=None):
    record.state = "COMPLETED"
    record.response_json = json.dumps(response_data, separators=(",", ":"), default=str)
    record.response_status = int(status_code)
    record.resource_type = resource_type
    record.resource_id = str(resource_id) if resource_id is not None else None


def _doctor_and_location(doctor_id, location_id):
    doctor = db.session.get(Clinician, doctor_id)
    location = db.session.get(ClinicLocation, location_id)
    if not doctor or not doctor.is_active:
        raise AppointmentAPIError("DOCTOR_NOT_FOUND", "Doctor was not found", 404)
    if not location or not location.is_active:
        raise AppointmentAPIError("INVALID_REQUEST", "Clinic location was not found", 404)
    if not location_accepts_appointments(location):
        raise AppointmentAPIError(
            "LOCATION_NOT_BOOKABLE",
            "Appointments cannot be booked at this hospital location; please visit or contact the hospital.",
            409,
        )
    return doctor, location


def _is_blocked(doctor_id, location_id, target_date, slot_time):
    rows = AppointmentSlotBlock.query.filter_by(
        clinician_id=doctor_id,
        location_id=location_id,
        block_date=target_date,
        is_active=True,
    ).all()
    for row in rows:
        if row.all_day:
            return True
        if row.start_time and row.end_time and row.start_time <= slot_time < row.end_time:
            return True
    return False


def _slot_count(doctor, location, target_date, slot_time, exclude_appointment_id=None):
    """Count active appointments for a date. ``slot_time=None`` counts the
    whole date (used by ARRIVAL_WINDOW capacity, which has no individual
    time), otherwise it counts only that exact discrete slot."""
    filters = [
        Appointment.appointment_date == target_date,
        Appointment.status.in_(ACTIVE_APPOINTMENT_STATUSES),
        or_(Appointment.is_deleted.is_(False), Appointment.is_deleted.is_(None)),
        or_(
            Appointment.clinician_id == doctor.id,
            and_(Appointment.clinician_id.is_(None), Appointment.doctor_name == doctor.display_name),
        ),
        or_(Appointment.location_id == location.id, Appointment.location_id.is_(None)),
    ]
    if slot_time is not None:
        filters.append(Appointment.appointment_time == slot_time)
    query = Appointment.query.filter(*filters)
    if exclude_appointment_id:
        query = query.filter(Appointment.id != exclude_appointment_id)
    return query.count()


def available_slots(doctor_id, location_id, target_date, *, now=None, exclude_appointment_id=None):
    doctor, location = _doctor_and_location(doctor_id, location_id)
    if not isinstance(target_date, date) or isinstance(target_date, datetime):
        raise AppointmentAPIError("INVALID_REQUEST", "date must use YYYY-MM-DD", 400)
    timezone_value, timezone_name = clinic_timezone()
    current = now.astimezone(timezone_value) if now and now.tzinfo else (now or datetime.now(timezone_value))
    if target_date < current.date() or target_date > current.date() + timedelta(days=90):
        raise AppointmentAPIError("INVALID_REQUEST", "date is outside the allowed booking range", 400)
    result, rule, _exceptions = doctor_schedule_resolution(location.id, doctor.id, target_date)
    if result.status is ClinicScheduleStatus.CLOSED:
        raise AppointmentAPIError("CLINIC_CLOSED", "Clinic or doctor is closed on this date", 409)
    if result.status is not ClinicScheduleStatus.OPEN or not result.arrival_window:
        raise AppointmentAPIError("DOCTOR_UNAVAILABLE", "Doctor schedule is not available", 409)
    duration = max(5, min(int(getattr(rule, "slot_duration_minutes", 20) or 20), 180))
    max_per_slot = max(1, min(int(getattr(rule, "max_patients_per_slot", 1) or 1), 20))
    cursor = datetime.combine(target_date, result.arrival_window.starts_at, tzinfo=timezone_value)
    ending = datetime.combine(target_date, result.arrival_window.ends_at, tzinfo=timezone_value)
    slots = []
    if not result.individual_time_slots:
        # ARRIVAL_WINDOW mode: one FCFS capacity pool for the whole window,
        # never an individually promised time. Shared with public booking.
        occupied = _slot_count(
            doctor,
            location,
            target_date,
            None,
            exclude_appointment_id=exclude_appointment_id,
        )
        blocked = _is_blocked(doctor.id, location.id, target_date, result.arrival_window.starts_at)
        in_future = ending >= current
        remaining = max(0, (result.daily_capacity or 0) - occupied) if in_future and not blocked else 0
        slots.append(
            {
                "start_at": cursor.isoformat(),
                "end_at": ending.isoformat(),
                "available": remaining > 0,
                "remaining_capacity": remaining,
            }
        )
    else:
        while cursor + timedelta(minutes=duration) <= ending:
            slot_time = cursor.time().replace(tzinfo=None)
            occupied = _slot_count(
                doctor,
                location,
                target_date,
                slot_time,
                exclude_appointment_id=exclude_appointment_id,
            )
            blocked = _is_blocked(doctor.id, location.id, target_date, slot_time)
            in_future = cursor >= current + timedelta(minutes=5)
            remaining = max(0, max_per_slot - occupied) if in_future and not blocked else 0
            slots.append(
                {
                    "start_at": cursor.isoformat(),
                    "end_at": (cursor + timedelta(minutes=duration)).isoformat(),
                    "available": remaining > 0,
                    "remaining_capacity": remaining,
                }
            )
            cursor += timedelta(minutes=duration)
    return {
        "date": target_date.isoformat(),
        "timezone": timezone_name,
        "doctor_id": doctor.id,
        "location_id": location.id,
        "individual_time_slots": bool(result.individual_time_slots),
        "slot_duration_minutes": duration,
        "max_patients_per_slot": max_per_slot,
        "slots": slots,
    }


def _parse_slot_start(value):
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise AppointmentAPIError("INVALID_REQUEST", "start_at must be ISO-8601", 400)
    if parsed.tzinfo is None:
        raise AppointmentAPIError("INVALID_REQUEST", "start_at must include a timezone offset", 400)
    timezone_value, _ = clinic_timezone()
    return parsed.astimezone(timezone_value)


def _acquire_slot_lock(db_session, doctor_id, location_id, target_date, slot_time):
    lock = AppointmentSlotLock.query.filter_by(
        clinician_id=doctor_id,
        location_id=location_id,
        appointment_date=target_date,
        slot_time=slot_time,
    ).first()
    if lock is None:
        try:
            with db_session.begin_nested():
                lock = AppointmentSlotLock(
                    clinician_id=doctor_id,
                    location_id=location_id,
                    appointment_date=target_date,
                    slot_time=slot_time,
                )
                db_session.add(lock)
                db_session.flush()
        except IntegrityError:
            lock = None
    query = AppointmentSlotLock.query.filter_by(
        clinician_id=doctor_id,
        location_id=location_id,
        appointment_date=target_date,
        slot_time=slot_time,
    )
    return query.with_for_update().one()


def _assert_slot_available(doctor_id, location_id, start_at, *, exclude_appointment_id=None):
    slot_data = available_slots(
        doctor_id,
        location_id,
        start_at.date(),
        exclude_appointment_id=exclude_appointment_id,
    )
    match = next((slot for slot in slot_data["slots"] if slot["start_at"] == start_at.isoformat()), None)
    if not match or not match["available"]:
        raise AppointmentAPIError("APPOINTMENT_UNAVAILABLE", "Requested slot is not available", 409)
    return match


def serialize_appointment(appointment):
    timezone_value, _ = clinic_timezone()
    start_at = appointment.slot_start_at
    end_at = appointment.slot_end_at
    if start_at and start_at.tzinfo is None:
        start_at = start_at.replace(tzinfo=timezone_value)
    if end_at and end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=timezone_value)
    return {
        "id": appointment.id,
        "appointment_no": appointment.appointment_no,
        "patient_id": appointment.patient_id,
        "doctor_id": appointment.clinician_id,
        "location_id": appointment.location_id,
        "date": appointment.appointment_date.isoformat(),
        "time": appointment.appointment_time.isoformat(timespec="minutes"),
        "start_at": start_at.isoformat() if start_at else None,
        "end_at": end_at.isoformat() if end_at else None,
        "status": appointment.status,
        "source": appointment.source,
        "late_arrival_status": appointment.late_arrival_status,
    }


def book_appointment(
    db_session,
    *,
    patient_session,
    doctor_id,
    location_id,
    start_at,
    reason=None,
    call_id=None,
    session_id=None,
    request_id=None,
):
    doctor, location = _doctor_and_location(doctor_id, location_id)
    patient = db_session.get(Patient, patient_session.patient_id)
    if patient is None:
        raise AppointmentAPIError("PATIENT_NOT_FOUND", "Patient was not found", 404)
    start = _parse_slot_start(start_at)
    _acquire_slot_lock(db_session, doctor.id, location.id, start.date(), start.time().replace(tzinfo=None))
    slot = _assert_slot_available(doctor.id, location.id, start)
    end = datetime.fromisoformat(slot["end_at"])
    appointment = Appointment(
        appointment_no="AIA-{}-{}".format(start.strftime("%Y%m%d"), secrets.token_hex(4).upper()),
        patient_id=patient.id,
        patient_name=patient.name,
        mobile=patient.mobile,
        age=patient.age,
        gender=patient.gender,
        doctor_name=doctor.display_name,
        clinician_id=doctor.id,
        location_id=location.id,
        appointment_date=start.date(),
        appointment_time=start.time().replace(tzinfo=None),
        slot_start_at=start.replace(tzinfo=None),
        slot_end_at=end.astimezone(start.tzinfo).replace(tzinfo=None),
        status="BOOKED",
        reason=str(reason or "")[:255] or None,
        payment_status="UNPAID",
        consultation_fee=float(doctor.consultation_fee or 0),
        source="AI_CALL",
        external_call_id=call_id,
        external_session_id=session_id,
        external_request_id=request_id,
        created_by="AI_CALL",
        is_deleted=False,
    )
    db_session.add(appointment)
    db_session.flush()
    return appointment


def find_or_create_patient_by_contact(db_session, *, name, mobile):
    """Finds an existing patient by phone, or creates one from just name +
    mobile -- the identification path AI-initiated NEW appointment booking
    uses instead of OTP-verified ``patient_session`` (see
    routes/ai_appointments_api.py's ``create_appointment``): clinic policy
    is that booking a new appointment needs the caller's name and mobile
    number only, never phone-possession proof. This intentionally reuses
    the exact same phone-normalization/lookup
    ``ai_patient_service.normalize_phone`` / ``_patient_for_phone`` the
    OTP-based identification flow already uses, so a caller who later does
    verify (e.g. to view existing appointments or reports, which still
    require it) resolves to this same ``Patient`` row rather than a
    duplicate. Never touches OTP/verification for any other endpoint.
    """
    try:
        from .ai_patient_service import PatientAccessError, _patient_for_phone, normalize_phone
    except ImportError:  # pragma: no cover
        from services.ai_patient_service import PatientAccessError, _patient_for_phone, normalize_phone

    clean_name = str(name or "").strip()
    if not (2 <= len(clean_name) <= 120):
        raise AppointmentAPIError("INVALID_REQUEST", "Patient name must be 2-120 characters", 400)
    try:
        normalized_mobile = normalize_phone(mobile)
    except PatientAccessError as exc:
        raise AppointmentAPIError(exc.code, exc.message, exc.status_code)

    patient = _patient_for_phone(normalized_mobile)
    if patient is None:
        patient = Patient(name=clean_name, mobile=normalized_mobile)
        db_session.add(patient)
        db_session.flush()
    elif not patient.name:
        patient.name = clean_name
    return patient


def book_appointment_with_contact(
    db_session,
    *,
    patient_name,
    mobile,
    doctor_id,
    location_id,
    start_at,
    reason=None,
    call_id=None,
    session_id=None,
    request_id=None,
):
    """Same booking logic as ``book_appointment`` above, for the
    unverified "just name + mobile, no OTP" AI booking path -- resolves a
    ``Patient`` row via ``find_or_create_patient_by_contact`` instead of
    requiring a ``patient_session``."""
    patient = find_or_create_patient_by_contact(db_session, name=patient_name, mobile=mobile)

    class _ContactOnlySession:
        patient_id = patient.id

    return book_appointment(
        db_session,
        patient_session=_ContactOnlySession(),
        doctor_id=doctor_id,
        location_id=location_id,
        start_at=start_at,
        reason=reason,
        call_id=call_id,
        session_id=session_id,
        request_id=request_id,
    )


def patient_appointments(patient_session, *, appointment_no=None, limit=20, offset=0):
    query = Appointment.query.filter(
        Appointment.patient_id == patient_session.patient_id,
        or_(Appointment.is_deleted.is_(False), Appointment.is_deleted.is_(None)),
    )
    if appointment_no:
        query = query.filter(Appointment.appointment_no == str(appointment_no).strip())
    rows = query.order_by(
        Appointment.appointment_date.desc(), Appointment.appointment_time.desc()
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 50))).all()
    return [serialize_appointment(row) for row in rows]


def owned_appointment(patient_session, appointment_no):
    appointment = Appointment.query.filter_by(
        appointment_no=str(appointment_no or "").strip(),
        patient_id=patient_session.patient_id,
    ).first()
    if not appointment or bool(appointment.is_deleted):
        raise AppointmentAPIError("APPOINTMENT_NOT_FOUND", "Appointment was not found", 404)
    return appointment


def cancel_appointment(appointment, *, reason=None, now=None):
    now = now or datetime.utcnow()
    if appointment.status == "CANCELLED":
        raise AppointmentAPIError("APPOINTMENT_ALREADY_CANCELLED", "Appointment is already cancelled", 409)
    if appointment.status in {"COMPLETED", "DELETED"}:
        raise AppointmentAPIError("INVALID_REQUEST", "Appointment can no longer be cancelled", 409)
    appointment.status = "CANCELLED"
    appointment.cancelled_at = now
    appointment.cancellation_reason = str(reason or "")[:255] or "Cancelled by verified caller"
    appointment.cancelled_by_source = "AI_CALL"
    return appointment


def reschedule_appointment(appointment, *, doctor_id, location_id, start_at):
    if appointment.status not in ACTIVE_APPOINTMENT_STATUSES:
        raise AppointmentAPIError("INVALID_REQUEST", "Appointment cannot be rescheduled", 409)
    doctor, location = _doctor_and_location(doctor_id, location_id)
    start = _parse_slot_start(start_at)
    _acquire_slot_lock(db.session, doctor.id, location.id, start.date(), start.time().replace(tzinfo=None))
    slot = _assert_slot_available(
        doctor.id,
        location.id,
        start,
        exclude_appointment_id=appointment.id,
    )
    end = datetime.fromisoformat(slot["end_at"])
    appointment.previous_appointment_date = appointment.appointment_date
    appointment.previous_appointment_time = appointment.appointment_time
    appointment.rescheduled_at = datetime.utcnow()
    appointment.clinician_id = doctor.id
    appointment.location_id = location.id
    appointment.doctor_name = doctor.display_name
    appointment.appointment_date = start.date()
    appointment.appointment_time = start.time().replace(tzinfo=None)
    appointment.slot_start_at = start.replace(tzinfo=None)
    appointment.slot_end_at = end.astimezone(start.tzinfo).replace(tzinfo=None)
    appointment.status = "BOOKED"
    appointment.source = "AI_CALL"
    return appointment


def mark_late_arrival(appointment, *, status="RUNNING_LATE", now=None):
    normalized = str(status or "RUNNING_LATE").upper()
    if normalized not in {"RUNNING_LATE", "ARRIVED_LATE"}:
        raise AppointmentAPIError("INVALID_REQUEST", "Invalid late-arrival status", 400)
    if appointment.status not in ACTIVE_APPOINTMENT_STATUSES:
        raise AppointmentAPIError("INVALID_REQUEST", "Appointment is not active", 409)
    appointment.late_arrival_status = normalized
    appointment.late_arrival_at = now or datetime.utcnow()
    if normalized == "ARRIVED_LATE":
        appointment.status = "LATE"
    return appointment


def add_to_waitlist(
    db_session,
    *,
    patient_session,
    doctor_id,
    location_id,
    preferred_date,
    preferred_start_time=None,
    preferred_end_time=None,
    call_id=None,
    session_id=None,
):
    _doctor_and_location(doctor_id, location_id)
    row = AppointmentWaitlist(
        waitlist_ref="WL-{}".format(secrets.token_hex(8).upper()),
        patient_id=patient_session.patient_id,
        clinician_id=doctor_id,
        location_id=location_id,
        preferred_date=preferred_date,
        preferred_start_time=preferred_start_time,
        preferred_end_time=preferred_end_time,
        status="WAITING",
        source="AI_CALL",
        external_call_id=call_id,
        external_session_id=session_id,
    )
    db_session.add(row)
    db_session.flush()
    return row
