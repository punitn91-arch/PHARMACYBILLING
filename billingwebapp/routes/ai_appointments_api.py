"""Scoped appointment slot and patient-owned mutation endpoints."""

from datetime import datetime

from flask import g, request

try:
    from ..models import db
    from ..services.ai_appointment_service import (
        AppointmentAPIError,
        add_to_waitlist,
        available_slots,
        begin_idempotency,
        book_appointment,
        cancel_appointment,
        complete_idempotency,
        mark_late_arrival,
        owned_appointment,
        patient_appointments,
        reschedule_appointment,
        serialize_appointment,
    )
    from .ai_api import (
        AIAPIError,
        ai_api_bp,
        enforce_api_rate_limit,
        json_object_body,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )
    from .ai_patient_api import verified_patient_session
except ImportError:  # pragma: no cover
    from models import db
    from services.ai_appointment_service import (
        AppointmentAPIError,
        add_to_waitlist,
        available_slots,
        begin_idempotency,
        book_appointment,
        cancel_appointment,
        complete_idempotency,
        mark_late_arrival,
        owned_appointment,
        patient_appointments,
        reschedule_appointment,
        serialize_appointment,
    )
    from routes.ai_api import (
        AIAPIError,
        ai_api_bp,
        enforce_api_rate_limit,
        json_object_body,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )
    from routes.ai_patient_api import verified_patient_session


def _translate(exc):
    raise AIAPIError(exc.code, exc.message, exc.status_code)


def _calendar_date(value, field_name="date"):
    try:
        return datetime.strptime(str(value or ""), "%Y-%m-%d").date()
    except ValueError:
        raise AIAPIError("INVALID_REQUEST", "{} must use YYYY-MM-DD".format(field_name), 400)


def _clock(value, field_name):
    if value in (None, ""):
        return None
    try:
        return datetime.strptime(str(value), "%H:%M").time()
    except ValueError:
        raise AIAPIError("INVALID_REQUEST", "{} must use HH:MM".format(field_name), 400)


@ai_api_bp.get("/appointments/slots")
@require_ai_scope("appointment:read")
def get_appointment_slots():
    enforce_api_rate_limit(60, window_seconds=60)
    try:
        data = available_slots(
            positive_int(request.args.get("doctor_id"), "doctor_id"),
            positive_int(request.args.get("location_id"), "location_id"),
            _calendar_date(request.args.get("date")),
        )
        start_time = _clock(request.args.get("start_time"), "start_time")
        end_time = _clock(request.args.get("end_time"), "end_time")
        if start_time and end_time and start_time >= end_time:
            raise AIAPIError("INVALID_REQUEST", "start_time must be before end_time", 400)
        period = str(request.args.get("period") or "").strip().lower()
        period_ranges = {
            "morning": (_clock("00:00", "period"), _clock("12:00", "period")),
            "afternoon": (_clock("12:00", "period"), _clock("17:00", "period")),
            "evening": (_clock("17:00", "period"), _clock("23:59", "period")),
        }
        if period and period not in period_ranges:
            raise AIAPIError("INVALID_REQUEST", "period must be morning, afternoon or evening", 400)
        if period:
            start_time, end_time = period_ranges[period]
        filtered = []
        for slot in data["slots"]:
            slot_time = datetime.fromisoformat(slot["start_at"]).time().replace(tzinfo=None)
            if not slot["available"]:
                continue
            if start_time and slot_time < start_time:
                continue
            if end_time and slot_time >= end_time:
                continue
            filtered.append(slot)
        data["slots"] = filtered
        data["available_slot_count"] = len(filtered)
        return success_response(data)
    except AppointmentAPIError as exc:
        _translate(exc)


@ai_api_bp.post("/appointments")
@require_ai_scope("appointment:create")
def create_appointment():
    set_ai_audit_action("APPOINTMENT_CREATE")
    payload = json_object_body()
    patient_session = verified_patient_session()
    replay_payload = {
        "patient_id": patient_session.patient_id,
        "doctor_id": payload.get("doctor_id"),
        "location_id": payload.get("location_id"),
        "start_at": payload.get("start_at"),
        "reason": str(payload.get("reason") or "")[:255],
    }
    try:
        record, replay, replay_status = begin_idempotency(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            operation="APPOINTMENT_CREATE",
            raw_key=request.headers.get("Idempotency-Key"),
            payload=replay_payload,
        )
        if replay is not None:
            return success_response(replay, replay_status)
        appointment = book_appointment(
            db.session,
            patient_session=patient_session,
            doctor_id=positive_int(payload.get("doctor_id"), "doctor_id"),
            location_id=positive_int(payload.get("location_id"), "location_id"),
            start_at=payload.get("start_at"),
            reason=payload.get("reason"),
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
            request_id=getattr(g, "ai_request_id", None),
        )
        result = serialize_appointment(appointment)
        complete_idempotency(
            record,
            result,
            201,
            resource_type="APPOINTMENT",
            resource_id=appointment.id,
        )
        set_ai_audit_action(
            "APPOINTMENT_CREATE", resource_type="APPOINTMENT", resource_id=appointment.id
        )
        db.session.commit()
        return success_response(result, 201)
    except AppointmentAPIError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.get("/appointments")
@require_ai_scope("appointment:read")
def lookup_appointments():
    set_ai_audit_action("APPOINTMENT_LOOKUP")
    patient_session = verified_patient_session()
    try:
        offset = nonnegative_int(request.args.get("offset", 0), "offset")
        items = patient_appointments(
            patient_session,
            appointment_no=str(request.args.get("appointment_no") or "").strip() or None,
            limit=min(50, positive_int(request.args.get("limit", 20), "limit")),
            offset=offset,
        )
        return success_response({"items": items, "count": len(items), "offset": offset})
    except AppointmentAPIError as exc:
        _translate(exc)


def _mutating_appointment_request(appointment_no, operation, action):
    set_ai_audit_action(operation)
    payload = json_object_body()
    patient_session = verified_patient_session()
    appointment = owned_appointment(patient_session, appointment_no)
    replay_input = dict(payload)
    replay_input.update({"appointment_no": appointment_no, "patient_id": patient_session.patient_id})
    record, replay, replay_status = begin_idempotency(
        db.session,
        api_client_id=g.ai_principal.api_client_id,
        operation=operation,
        raw_key=request.headers.get("Idempotency-Key"),
        payload=replay_input,
    )
    if replay is not None:
        return success_response(replay, replay_status)
    action(appointment, payload)
    appointment.external_call_id = getattr(g, "ai_call_id", None)
    appointment.external_session_id = getattr(g, "ai_session_id", None)
    appointment.external_request_id = getattr(g, "ai_request_id", None)
    result = serialize_appointment(appointment)
    complete_idempotency(
        record,
        result,
        200,
        resource_type="APPOINTMENT",
        resource_id=appointment.id,
    )
    set_ai_audit_action(operation, resource_type="APPOINTMENT", resource_id=appointment.id)
    db.session.commit()
    return success_response(result)


@ai_api_bp.post("/appointments/<appointment_no>/cancel")
@require_ai_scope("appointment:update")
def cancel_existing_appointment(appointment_no):
    try:
        return _mutating_appointment_request(
            appointment_no,
            "APPOINTMENT_CANCEL",
            lambda appointment, payload: cancel_appointment(
                appointment, reason=payload.get("reason")
            ),
        )
    except AppointmentAPIError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/appointments/<appointment_no>/reschedule")
@require_ai_scope("appointment:update")
def reschedule_existing_appointment(appointment_no):
    try:
        return _mutating_appointment_request(
            appointment_no,
            "APPOINTMENT_RESCHEDULE",
            lambda appointment, payload: reschedule_appointment(
                appointment,
                doctor_id=positive_int(payload.get("doctor_id"), "doctor_id"),
                location_id=positive_int(payload.get("location_id"), "location_id"),
                start_at=payload.get("start_at"),
            ),
        )
    except AppointmentAPIError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/appointments/<appointment_no>/late-arrival")
@require_ai_scope("appointment:update")
def update_late_arrival(appointment_no):
    try:
        return _mutating_appointment_request(
            appointment_no,
            "APPOINTMENT_LATE_ARRIVAL",
            lambda appointment, payload: mark_late_arrival(
                appointment, status=payload.get("status", "RUNNING_LATE")
            ),
        )
    except AppointmentAPIError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/appointments/waitlist")
@require_ai_scope("appointment:create")
def create_waitlist_entry():
    set_ai_audit_action("WAITLIST_CREATE")
    payload = json_object_body()
    patient_session = verified_patient_session()
    replay_input = dict(payload)
    replay_input["patient_id"] = patient_session.patient_id
    try:
        record, replay, replay_status = begin_idempotency(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            operation="WAITLIST_CREATE",
            raw_key=request.headers.get("Idempotency-Key"),
            payload=replay_input,
        )
        if replay is not None:
            return success_response(replay, replay_status)
        row = add_to_waitlist(
            db.session,
            patient_session=patient_session,
            doctor_id=positive_int(payload.get("doctor_id"), "doctor_id"),
            location_id=positive_int(payload.get("location_id"), "location_id"),
            preferred_date=_calendar_date(payload.get("preferred_date"), "preferred_date"),
            preferred_start_time=_clock(payload.get("preferred_start_time"), "preferred_start_time"),
            preferred_end_time=_clock(payload.get("preferred_end_time"), "preferred_end_time"),
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
        )
        result = {"waitlist_ref": row.waitlist_ref, "status": row.status}
        complete_idempotency(record, result, 201, resource_type="WAITLIST", resource_id=row.id)
        set_ai_audit_action("WAITLIST_CREATE", resource_type="WAITLIST", resource_id=row.id)
        db.session.commit()
        return success_response(result, 201)
    except AppointmentAPIError as exc:
        db.session.rollback()
        _translate(exc)
