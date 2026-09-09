"""Callback, complaint and approved-template notification endpoints."""

from datetime import datetime, timezone

from flask import current_app, g, request

try:
    from ..models import Appointment, ClinicLocation, Clinician, db
    from ..services.ai_appointment_service import (
        AppointmentAPIError,
        begin_idempotency,
        complete_idempotency,
    )
    from ..services.ai_notification_service import NotificationError, send_template_notification
    from ..services.ai_operations_service import OperationsError, create_callback, create_complaint
    from ..services.ai_clinic_service import active_profile
    from .ai_api import AIAPIError, ai_api_bp, json_object_body, require_ai_scope, set_ai_audit_action, success_response
    from .ai_patient_api import verified_patient_session
except ImportError:  # pragma: no cover
    from models import Appointment, ClinicLocation, Clinician, db
    from services.ai_appointment_service import (
        AppointmentAPIError,
        begin_idempotency,
        complete_idempotency,
    )
    from services.ai_notification_service import NotificationError, send_template_notification
    from services.ai_operations_service import OperationsError, create_callback, create_complaint
    from services.ai_clinic_service import active_profile
    from routes.ai_api import AIAPIError, ai_api_bp, json_object_body, require_ai_scope, set_ai_audit_action, success_response
    from routes.ai_patient_api import verified_patient_session


def _optional_patient_session():
    if not str(request.headers.get("X-Patient-Session") or "").strip():
        return None
    return verified_patient_session()


def _preferred_datetime(value):
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise AIAPIError("INVALID_REQUEST", "preferred_at must be ISO-8601", 400)
    if parsed.tzinfo is None:
        raise AIAPIError("INVALID_REQUEST", "preferred_at must include a timezone offset", 400)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _translate(exc):
    raise AIAPIError(exc.code, exc.message, exc.status_code)


@ai_api_bp.post("/callbacks")
@require_ai_scope("callback:create")
def create_callback_request():
    set_ai_audit_action("CALLBACK_CREATE")
    if not bool(current_app.config.get("AI_CALLBACKS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI callback intake is disabled", 503)
    payload = json_object_body()
    patient_session = _optional_patient_session()
    replay_input = dict(payload)
    replay_input["patient_id"] = patient_session.patient_id if patient_session else None
    try:
        record, replay, replay_status = begin_idempotency(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            operation="CALLBACK_CREATE",
            raw_key=request.headers.get("Idempotency-Key"),
            payload=replay_input,
        )
        if replay is not None:
            return success_response(replay, replay_status)
        row = create_callback(
            db.session,
            mobile=payload.get("mobile"),
            reason=payload.get("reason"),
            caller_name=payload.get("caller_name"),
            category=payload.get("category"),
            priority=payload.get("priority", "NORMAL"),
            ai_summary=payload.get("ai_summary"),
            preferred_at=_preferred_datetime(payload.get("preferred_at")),
            patient_id=patient_session.patient_id if patient_session else None,
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
        )
        result = {"callback_ref": row.callback_ref, "status": row.status}
        complete_idempotency(record, result, 201, resource_type="CALLBACK", resource_id=row.id)
        set_ai_audit_action("CALLBACK_CREATE", resource_type="CALLBACK", resource_id=row.id)
        db.session.commit()
        return success_response(result, 201)
    except (OperationsError, AppointmentAPIError) as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/complaints")
@require_ai_scope("complaint:create")
def create_complaint_request():
    set_ai_audit_action("COMPLAINT_CREATE")
    if not bool(current_app.config.get("AI_COMPLAINTS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI complaint intake is disabled", 503)
    payload = json_object_body()
    patient_session = _optional_patient_session()
    replay_input = dict(payload)
    replay_input["patient_id"] = patient_session.patient_id if patient_session else None
    try:
        record, replay, replay_status = begin_idempotency(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            operation="COMPLAINT_CREATE",
            raw_key=request.headers.get("Idempotency-Key"),
            payload=replay_input,
        )
        if replay is not None:
            return success_response(replay, replay_status)
        row = create_complaint(
            db.session,
            mobile=payload.get("mobile"),
            category=payload.get("category"),
            summary=payload.get("summary"),
            details=payload.get("details"),
            priority=payload.get("priority", "NORMAL"),
            caller_name=payload.get("caller_name"),
            patient_id=patient_session.patient_id if patient_session else None,
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
        )
        result = {"complaint_ref": row.complaint_ref, "status": row.status}
        complete_idempotency(record, result, 201, resource_type="COMPLAINT", resource_id=row.id)
        set_ai_audit_action("COMPLAINT_CREATE", resource_type="COMPLAINT", resource_id=row.id)
        db.session.commit()
        return success_response(result, 201)
    except (OperationsError, AppointmentAPIError) as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/notifications")
@require_ai_scope("notification:send")
def send_notification():
    set_ai_audit_action("NOTIFICATION_SEND")
    if not bool(current_app.config.get("AI_NOTIFICATIONS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI notifications are disabled", 503)
    payload = json_object_body()
    patient_session = verified_patient_session()
    recipient = str(payload.get("recipient") or "").strip()
    # The API cannot become an arbitrary messaging relay. Only the phone that
    # owns the verified patient session may receive a message.
    if recipient.replace(" ", "") not in {
        patient_session.mobile,
        patient_session.mobile.lstrip("+"),
        patient_session.mobile[-10:],
    }:
        raise AIAPIError("FORBIDDEN", "Notification recipient is not verified", 403)
    try:
        delivery, duplicate = send_template_notification(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            event_code=payload.get("event_code"),
            channel=payload.get("channel"),
            language=payload.get("language", "en"),
            recipient=patient_session.mobile,
            variables=payload.get("variables") if isinstance(payload.get("variables"), dict) else {},
            idempotency_key=request.headers.get("Idempotency-Key"),
            fingerprint_secret=current_app.config.get("AI_AUDIT_FINGERPRINT_SECRET"),
            testing=bool(current_app.config.get("TESTING")),
        )
        db.session.commit()
        set_ai_audit_action(
            "NOTIFICATION_SEND", resource_type="NOTIFICATION", resource_id=delivery.id
        )
        return success_response(
            {"delivery_id": delivery.id, "status": delivery.status, "duplicate": duplicate},
            200 if duplicate else 202,
        )
    except NotificationError as exc:
        db.session.rollback()
        _translate(exc)


def _send_fixed_notification(*, patient_session, event_code, variables, payload):
    delivery, duplicate = send_template_notification(
        db.session,
        api_client_id=g.ai_principal.api_client_id,
        event_code=event_code,
        channel=payload.get("channel", "SMS"),
        language=payload.get("language", "en"),
        recipient=patient_session.mobile,
        variables=variables,
        idempotency_key=request.headers.get("Idempotency-Key"),
        fingerprint_secret=current_app.config.get("AI_AUDIT_FINGERPRINT_SECRET"),
        testing=bool(current_app.config.get("TESTING")),
    )
    db.session.commit()
    set_ai_audit_action(
        "NOTIFICATION_SEND", resource_type="NOTIFICATION", resource_id=delivery.id
    )
    return success_response(
        {"delivery_id": delivery.id, "status": delivery.status, "duplicate": duplicate},
        200 if duplicate else 202,
    )


@ai_api_bp.post("/locations/<int:location_id>/send")
@require_ai_scope("notification:send")
def send_location(location_id):
    set_ai_audit_action("LOCATION_SEND", resource_type="LOCATION", resource_id=location_id)
    if not bool(current_app.config.get("AI_NOTIFICATIONS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI notifications are disabled", 503)
    payload = json_object_body()
    patient_session = verified_patient_session()
    location = ClinicLocation.query.filter_by(id=location_id, is_active=True).first()
    if not location:
        raise AIAPIError("INVALID_REQUEST", "Clinic location was not found", 404)
    profile = active_profile()
    try:
        return _send_fixed_notification(
            patient_session=patient_session,
            event_code="CLINIC_LOCATION",
            variables={
                "clinic_name": profile.clinic_name if profile else "Clinic",
                "clinic_phone": location.public_phone or (profile.phone if profile else ""),
                "location_name": location.display_name,
                "maps_url": location.maps_url or location.public_address or "",
            },
            payload=payload,
        )
    except NotificationError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/notifications/appointment")
@require_ai_scope("notification:send")
def send_appointment_notification():
    set_ai_audit_action("APPOINTMENT_NOTIFICATION")
    if not bool(current_app.config.get("AI_NOTIFICATIONS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI notifications are disabled", 503)
    payload = json_object_body()
    patient_session = verified_patient_session()
    appointment_no = str(payload.get("appointment_no") or "").strip()
    appointment = Appointment.query.filter_by(
        appointment_no=appointment_no, patient_id=patient_session.patient_id
    ).first()
    if not appointment or bool(appointment.is_deleted):
        raise AIAPIError("APPOINTMENT_NOT_FOUND", "Appointment was not found", 404)
    doctor = db.session.get(Clinician, appointment.clinician_id) if appointment.clinician_id else None
    location = db.session.get(ClinicLocation, appointment.location_id) if appointment.location_id else None
    profile = active_profile()
    set_ai_audit_action(
        "APPOINTMENT_NOTIFICATION", resource_type="APPOINTMENT", resource_id=appointment.id
    )
    try:
        return _send_fixed_notification(
            patient_session=patient_session,
            event_code="APPOINTMENT_CONFIRMATION",
            variables={
                "appointment_no": appointment.appointment_no,
                "doctor_name": doctor.display_name if doctor else appointment.doctor_name,
                "appointment_date": appointment.appointment_date.isoformat(),
                "appointment_time": appointment.appointment_time.isoformat(timespec="minutes"),
                "clinic_name": profile.clinic_name if profile else "Clinic",
                "location_name": location.display_name if location else "",
            },
            payload=payload,
        )
    except NotificationError as exc:
        db.session.rollback()
        _translate(exc)
