"""Narrow, authenticated OmniDimension adapter for the clinic voice agent.

This is intentionally *not* a proxy to the general AI API.  OmniDimension
Custom API actions use a long-lived static header, while the internal AI API
uses short-lived, scoped bearer tokens and patient verification sessions.  A
small adapter lets the voice agent answer public questions and create
staff-reviewed requests without giving it patient records, OTPs, report files
or an internal API client secret.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from urllib.parse import urlsplit

from flask import Blueprint, current_app, g, jsonify, request
from sqlalchemy.exc import IntegrityError

try:
    from ..models import (
        AIAPIRequestAudit,
        ClinicLocation,
        Clinician,
        OmnidimGatewayAction,
        db,
    )
    from ..services.ai_appointment_service import AppointmentAPIError, available_slots
    from ..services.ai_clinic_service import (
        clinic_information,
        clinic_timezone,
        doctor_schedule_resolution,
        schedule_payload,
        serialize_clinician,
    )
    from ..services.ai_operations_service import OperationsError, create_callback, create_complaint
except ImportError:  # pragma: no cover - direct ``python app.py`` execution
    from models import AIAPIRequestAudit, ClinicLocation, Clinician, OmnidimGatewayAction, db
    from services.ai_appointment_service import AppointmentAPIError, available_slots
    from services.ai_clinic_service import (
        clinic_information,
        clinic_timezone,
        doctor_schedule_resolution,
        schedule_payload,
        serialize_clinician,
    )
    from services.ai_operations_service import OperationsError, create_callback, create_complaint


OMNIDIM_GATEWAY_PREFIX = "/api/v1/omnidim"
omnidim_gateway_bp = Blueprint("omnidim_gateway", __name__, url_prefix=OMNIDIM_GATEWAY_PREFIX)

_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
_SAFE_CONTEXT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_MAX_REQUEST_BYTES = 16 * 1024


class OmnidimGatewayError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)
        self.status_code = int(status_code)


def _env_flag(value, default=False):
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _configured_secret():
    value = str(current_app.config.get("OMNIDIM_GATEWAY_SECRET") or "")
    if len(value.encode("utf-8")) < 32:
        return None
    return value


def _request_id():
    value = str(request.headers.get("X-Request-ID") or "").strip()
    return value if _SAFE_REQUEST_ID.fullmatch(value) else str(uuid.uuid4())


def _safe_context_value(value, field_name):
    value = str(value or "").strip()
    if not value:
        return None
    if not _SAFE_CONTEXT_ID.fullmatch(value):
        raise OmnidimGatewayError("INVALID_REQUEST", "{} is invalid".format(field_name), 400)
    return value


def _remote_addr():
    if _env_flag(current_app.config.get("OMNIDIM_GATEWAY_TRUST_PROXY_HEADERS"), False):
        forwarded_for = (request.headers.get("X-Forwarded-For") or "").split(",", 1)[0].strip()
        if forwarded_for:
            return forwarded_for
    return request.remote_addr or ""


def _audit_fingerprint(value):
    secret = (
        current_app.config.get("AI_AUDIT_FINGERPRINT_SECRET")
        or current_app.secret_key
        or "local-omnidim-audit-fingerprint"
    )
    return hmac.new(
        str(secret).encode("utf-8"),
        str(value or "").encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _secret_digest(value):
    secret = _configured_secret()
    if not secret:  # The feature gate reports a configuration-safe message first.
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Voice gateway is unavailable", 503)
    return hmac.new(secret.encode("utf-8"), str(value or "").encode("utf-8"), hashlib.sha256).hexdigest()


def _safe_text(value, field_name, maximum, *, required=False):
    cleaned = str(value or "").strip()
    if required and not cleaned:
        raise OmnidimGatewayError("INVALID_REQUEST", "{} is required".format(field_name), 400)
    if len(cleaned) > maximum:
        raise OmnidimGatewayError(
            "INVALID_REQUEST", "{} must be at most {} characters".format(field_name, maximum), 400
        )
    return cleaned or None


def _normalise_name(value):
    return " ".join(str(value or "").casefold().split())


def _calendar_date(value, field_name="date", *, required=True):
    raw = _safe_text(value, field_name, 10, required=required)
    if raw is None:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise OmnidimGatewayError("INVALID_REQUEST", "{} must use YYYY-MM-DD".format(field_name), 400)


def _preferred_datetime(value):
    raw = _safe_text(value, "preferred_at", 40)
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise OmnidimGatewayError("INVALID_REQUEST", "preferred_at must use ISO-8601", 400)
    if parsed.tzinfo is None:
        raise OmnidimGatewayError(
            "INVALID_REQUEST", "preferred_at must include a timezone offset", 400
        )
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _find_doctor(value, *, required=True):
    requested = _safe_text(value, "doctor_name", 160, required=required)
    if requested is None:
        return None
    key = _normalise_name(requested)
    matches = []
    for doctor in Clinician.query.filter_by(is_active=True).order_by(Clinician.id.asc()).all():
        aliases = {
            _normalise_name(doctor.display_name),
            _normalise_name(doctor.code),
            _normalise_name("{} {}".format(doctor.public_title or "", doctor.display_name or "")),
        }
        if key in aliases:
            matches.append(doctor)
    if not matches:
        raise OmnidimGatewayError("DOCTOR_NOT_FOUND", "Requested doctor was not found", 404)
    if len(matches) > 1:
        raise OmnidimGatewayError("DOCTOR_AMBIGUOUS", "Please use the doctor's full name", 409)
    return matches[0]


def _find_location(value, *, doctor=None, required=True):
    requested = _safe_text(value, "location_name", 160, required=False)
    if requested is None and doctor is not None and doctor.default_location_id:
        location = db.session.get(ClinicLocation, doctor.default_location_id)
        if location and location.is_active:
            return location
    if requested is None:
        if required:
            raise OmnidimGatewayError("LOCATION_REQUIRED", "Clinic location is required", 400)
        return None
    key = _normalise_name(requested)
    matches = []
    for location in ClinicLocation.query.filter_by(is_active=True).order_by(ClinicLocation.id.asc()).all():
        if key in {_normalise_name(location.display_name), _normalise_name(location.code)}:
            matches.append(location)
    if not matches:
        raise OmnidimGatewayError("LOCATION_NOT_FOUND", "Requested clinic location was not found", 404)
    if len(matches) > 1:
        raise OmnidimGatewayError("LOCATION_AMBIGUOUS", "Please use the full clinic location name", 409)
    return matches[0]


def _capture_context(payload=None):
    if payload and isinstance(payload, dict):
        body_call_id = _safe_context_value(payload.get("call_id"), "call_id")
        body_session_id = _safe_context_value(payload.get("session_id"), "session_id")
        if body_call_id:
            g.omnidim_call_id = body_call_id
        if body_session_id:
            g.omnidim_session_id = body_session_id
    return getattr(g, "omnidim_call_id", None), getattr(g, "omnidim_session_id", None)


def _gateway_base_url(*, booking=False):
    configured = ""
    if booking:
        configured = str(os.environ.get("PUBLIC_BOOKING_BASE_URL") or "").strip()
    configured = configured or str(current_app.config.get("APPLICATION_BASE_URL") or "").strip()
    if not configured and not bool(current_app.config.get("IS_PRODUCTION", False)):
        configured = request.url_root.rstrip("/")
    parsed = urlsplit(configured)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Public clinic URL is not configured", 503)
    if bool(current_app.config.get("IS_PRODUCTION", False)) and parsed.scheme != "https":
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Public clinic URL is not configured", 503)
    return "{}://{}{}".format(parsed.scheme, parsed.netloc, parsed.path.rstrip("/"))


def _public_url(path, *, booking=False):
    return "{}{}".format(_gateway_base_url(booking=booking), path)


def _json_object_body():
    if request.content_length and request.content_length > _MAX_REQUEST_BYTES:
        raise OmnidimGatewayError("REQUEST_TOO_LARGE", "Request body is too large", 413)
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        raise OmnidimGatewayError("INVALID_REQUEST", "A JSON object request body is required", 400)
    return payload


def _require_confirmation(payload):
    if payload.get("confirmed") is not True:
        raise OmnidimGatewayError(
            "CONFIRMATION_REQUIRED", "Caller confirmation is required before creating a request", 400
        )


def _idempotency_payload(payload):
    return {key: value for key, value in payload.items() if key != "request_key"}


def _existing_action(operation, key_hash, fingerprint):
    row = OmnidimGatewayAction.query.filter_by(
        operation=operation,
        idempotency_key_hash=key_hash,
    ).first()
    if not row:
        return None
    if not hmac.compare_digest(str(row.request_fingerprint), str(fingerprint)):
        raise OmnidimGatewayError(
            "IDEMPOTENCY_CONFLICT", "request_key cannot be reused for a different request", 409
        )
    if row.state != "COMPLETED" or not row.response_json or not row.response_status:
        raise OmnidimGatewayError("REQUEST_IN_PROGRESS", "Request is already being processed", 409)
    try:
        result = json.loads(row.response_json)
    except (TypeError, ValueError):
        raise OmnidimGatewayError("REQUEST_IN_PROGRESS", "Request is already being processed", 409)
    if not isinstance(result, dict):
        raise OmnidimGatewayError("REQUEST_IN_PROGRESS", "Request is already being processed", 409)
    return result, int(row.response_status), row


def _run_idempotent_action(operation, payload, execute):
    raw_key = _safe_text(payload.get("request_key"), "request_key", 128, required=True)
    if not _SAFE_IDEMPOTENCY_KEY.fullmatch(raw_key):
        raise OmnidimGatewayError("INVALID_REQUEST", "request_key is invalid", 400)
    key_hash = _secret_digest("idempotency:{}".format(raw_key))
    canonical_body = json.dumps(
        _idempotency_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    fingerprint = _secret_digest("request:{}".format(canonical_body))
    existing = _existing_action(operation, key_hash, fingerprint)
    if existing:
        result, status_code, row = existing
        return result, status_code, True, row

    row = OmnidimGatewayAction(
        operation=operation,
        idempotency_key_hash=key_hash,
        request_fingerprint=fingerprint,
        state="IN_PROGRESS",
    )
    db.session.add(row)
    try:
        db.session.flush()
    except IntegrityError:
        # A concurrent retry may have won the unique key race.  It has not
        # performed the business action in this transaction, so retry lookup
        # safely after rolling this tentative insert back.
        db.session.rollback()
        existing = _existing_action(operation, key_hash, fingerprint)
        if existing:
            result, status_code, replay_row = existing
            return result, status_code, True, replay_row
        raise OmnidimGatewayError("REQUEST_IN_PROGRESS", "Request is already being processed", 409)

    result, status_code, resource_type, resource_id = execute()
    row.state = "COMPLETED"
    row.response_json = json.dumps(result, sort_keys=True, separators=(",", ":"))
    row.response_status = int(status_code)
    row.resource_type = resource_type
    row.resource_id = str(resource_id)[:80] if resource_id is not None else None
    row.completed_at = datetime.utcnow()
    db.session.commit()
    return result, int(status_code), False, row


def _success(data=None, status_code=200):
    response = jsonify(
        {
            "success": True,
            "data": {} if data is None else data,
            "request_id": getattr(g, "omnidim_request_id", None),
        }
    )
    response.status_code = int(status_code)
    return response


def _error(code, message, status_code):
    g.omnidim_error_code = str(code)
    response = jsonify(
        {
            "success": False,
            "error": {"code": str(code), "message": str(message)},
            "request_id": getattr(g, "omnidim_request_id", None),
        }
    )
    response.status_code = int(status_code)
    return response


def _set_audit_action(action, *, resource_type=None, resource_id=None):
    g.omnidim_action = str(action or "")[:80] or None
    g.omnidim_resource_type = str(resource_type or "")[:50] or None
    g.omnidim_resource_id = str(resource_id)[:80] if resource_id is not None else None


def _enforce_rate_limit():
    try:
        limit = int(current_app.config.get("OMNIDIM_GATEWAY_RATE_LIMIT_PER_MINUTE", 60))
    except (TypeError, ValueError):
        limit = 60
    limit = max(5, min(limit, 300))
    recent_count = AIAPIRequestAudit.query.filter(
        AIAPIRequestAudit.endpoint == request.path,
        AIAPIRequestAudit.created_at >= datetime.utcnow() - timedelta(minutes=1),
    ).count()
    if recent_count >= limit:
        raise OmnidimGatewayError("RATE_LIMITED", "Too many requests", 429)


@omnidim_gateway_bp.before_request
def prepare_gateway_request():
    g.omnidim_started_at = time.perf_counter()
    g.omnidim_request_id = _request_id()
    g.omnidim_call_id = _safe_context_value(
        request.headers.get("X-Omnidim-Call-ID"), "X-Omnidim-Call-ID"
    )
    g.omnidim_session_id = _safe_context_value(
        request.headers.get("X-Omnidim-Session-ID"), "X-Omnidim-Session-ID"
    )
    g.omnidim_error_code = None
    g.omnidim_action = None
    g.omnidim_resource_type = None
    g.omnidim_resource_id = None
    if not bool(current_app.config.get("OMNIDIM_GATEWAY_ENABLED", False)):
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Voice gateway is unavailable", 503)
    secret = _configured_secret()
    supplied = str(request.headers.get("X-Clinic-Gateway-Key") or "")
    if not secret:
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Voice gateway is unavailable", 503)
    if not supplied or not hmac.compare_digest(supplied, secret):
        raise OmnidimGatewayError("UNAUTHORIZED", "Authentication failed", 401)
    _enforce_rate_limit()


@omnidim_gateway_bp.after_request
def finalize_gateway_request(response):
    response.headers["X-Request-ID"] = getattr(g, "omnidim_request_id", "")
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    try:
        elapsed = max(0, int((time.perf_counter() - g.omnidim_started_at) * 1000))
        audit = AIAPIRequestAudit(
            api_client_id=None,
            request_id=g.omnidim_request_id,
            call_id=getattr(g, "omnidim_call_id", None),
            session_id=getattr(g, "omnidim_session_id", None),
            method=request.method[:10],
            endpoint=request.path[:255],
            status_code=int(response.status_code),
            outcome="SUCCESS" if response.status_code < 400 else "FAILURE",
            error_code=getattr(g, "omnidim_error_code", None),
            action=getattr(g, "omnidim_action", None),
            resource_type=getattr(g, "omnidim_resource_type", None),
            resource_id=getattr(g, "omnidim_resource_id", None),
            latency_ms=elapsed,
            client_ip_fingerprint=_audit_fingerprint(_remote_addr()),
        )
        db.session.add(audit)
        db.session.commit()
        current_app.logger.info(
            "omnidim_gateway_request %s",
            json.dumps(
                {
                    "request_id": g.omnidim_request_id,
                    "call_id": getattr(g, "omnidim_call_id", None),
                    "endpoint": request.path,
                    "method": request.method,
                    "status_code": response.status_code,
                    "latency_ms": elapsed,
                    "error_code": getattr(g, "omnidim_error_code", None),
                },
                separators=(",", ":"),
            ),
        )
    except Exception:
        db.session.rollback()
        current_app.logger.exception(
            "OmniDimension gateway audit persistence failed for request_id=%s",
            getattr(g, "omnidim_request_id", "unknown"),
        )
    return response


@omnidim_gateway_bp.errorhandler(OmnidimGatewayError)
def handle_gateway_error(exc):
    db.session.rollback()
    return _error(exc.code, exc.message, exc.status_code)


@omnidim_gateway_bp.errorhandler(404)
def handle_gateway_not_found(_exc):
    return _error("NOT_FOUND", "Gateway endpoint was not found", 404)


@omnidim_gateway_bp.errorhandler(405)
def handle_gateway_method_not_allowed(_exc):
    return _error("METHOD_NOT_ALLOWED", "HTTP method is not allowed", 405)


@omnidim_gateway_bp.errorhandler(Exception)
def handle_gateway_unexpected_error(exc):
    db.session.rollback()
    current_app.logger.exception(
        "Unhandled OmniDimension gateway error request_id=%s",
        getattr(g, "omnidim_request_id", "unknown"),
    )
    return _error("INTERNAL_ERROR", "The request could not be completed", 500)


@omnidim_gateway_bp.get("/clinic-info")
def get_clinic_info():
    _set_audit_action("OMNIDIM_CLINIC_INFO")
    source = clinic_information()
    clinic = source["clinic"]
    # Keep the response limited to information that clinic staff already make
    # public.  In particular, no patient or internal user records are present.
    return _success(
        {
            "configured": source["configured"],
            "clinic": {
                "name": clinic["name"],
                "phone": clinic["phone"],
                "reception_phone": clinic["reception_phone"],
                "website_url": clinic["website_url"],
                "emergency_wording": clinic["emergency_wording"],
                "consultation_information": clinic["consultation_information"],
                "general_policies": clinic["general_policies"],
                "payment_methods": clinic["payment_methods"],
                "available_services": clinic["available_services"],
                "timezone": clinic["timezone"],
            },
            "locations": source["locations"],
        }
    )


@omnidim_gateway_bp.get("/doctors")
def get_doctors():
    _set_audit_action("OMNIDIM_DOCTOR_LIST")
    items = [
        serialize_clinician(row)
        for row in Clinician.query.filter_by(is_active=True).order_by(Clinician.display_name.asc()).all()
    ]
    return _success({"items": items, "count": len(items)})


@omnidim_gateway_bp.get("/timings")
def get_timings():
    _set_audit_action("OMNIDIM_TIMING_LOOKUP")
    target_date = _calendar_date(request.args.get("date"))
    doctor = _find_doctor(request.args.get("doctor_name"))
    location = _find_location(request.args.get("location_name"), doctor=doctor)
    result, rule, _exceptions = doctor_schedule_resolution(location.id, doctor.id, target_date)
    timing = schedule_payload(
        result,
        target_date,
        rule=rule,
        location_id=location.id,
        clinician_id=doctor.id,
    )
    timing["timezone"] = clinic_timezone()[1]
    return _success(
        {
            "doctor": {"id": doctor.id, "name": doctor.display_name},
            "location": {"id": location.id, "name": location.display_name},
            "timing": timing,
        }
    )


@omnidim_gateway_bp.get("/appointment-slots")
def get_appointment_slots():
    _set_audit_action("OMNIDIM_SLOT_LOOKUP")
    target_date = _calendar_date(request.args.get("date"))
    doctor = _find_doctor(request.args.get("doctor_name"))
    location = _find_location(request.args.get("location_name"), doctor=doctor)
    try:
        data = available_slots(doctor.id, location.id, target_date)
    except AppointmentAPIError as exc:
        raise OmnidimGatewayError(exc.code, exc.message, exc.status_code)
    available = [slot for slot in data["slots"] if slot.get("available")]
    return _success(
        {
            "doctor": {"id": doctor.id, "name": doctor.display_name},
            "location": {"id": location.id, "name": location.display_name},
            "date": data["date"],
            "timezone": data["timezone"],
            "individual_time_slots": data["individual_time_slots"],
            "slots": available,
            "available_slot_count": len(available),
        }
    )


@omnidim_gateway_bp.get("/appointment-booking-link")
def get_appointment_booking_link():
    _set_audit_action("OMNIDIM_APPOINTMENT_PORTAL")
    return _success(
        {
            "url": _public_url("/book-appointment", booking=True),
            "verification_required": True,
            "message": "Appointment confirmation is completed through the clinic's secure OTP verification flow.",
        }
    )


@omnidim_gateway_bp.get("/lab-report-access")
def get_lab_report_access():
    _set_audit_action("OMNIDIM_REPORT_PORTAL")
    return _success(
        {
            "url": _public_url("/my-lab-reports"),
            "verification_required": True,
            "message": "Lab reports are available only after verification in the secure patient portal.",
        }
    )


def _appointment_request_summary(payload, doctor, location):
    requested_date = _calendar_date(payload.get("preferred_date"), "preferred_date", required=False)
    requested_time = _safe_text(payload.get("preferred_time"), "preferred_time", 5)
    if requested_time:
        try:
            datetime.strptime(requested_time, "%H:%M")
        except ValueError:
            raise OmnidimGatewayError("INVALID_REQUEST", "preferred_time must use HH:MM", 400)
    parts = ["Appointment request via voice assistant"]
    if doctor:
        parts.append("Doctor: {}".format(doctor.display_name))
    if location:
        parts.append("Location: {}".format(location.display_name))
    if requested_date:
        parts.append("Preferred date: {}".format(requested_date.isoformat()))
    if requested_time:
        parts.append("Preferred time: {}".format(requested_time))
    reason = _safe_text(payload.get("reason"), "reason", 240, required=True)
    parts.append("Reason: {}".format(reason))
    result = "; ".join(parts)
    return result[:500], requested_date


@omnidim_gateway_bp.post("/appointment-requests")
def create_appointment_request():
    _set_audit_action("OMNIDIM_APPOINTMENT_REQUEST")
    if not bool(current_app.config.get("AI_CALLBACKS_ENABLED", False)):
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Appointment request intake is disabled", 503)
    payload = _json_object_body()
    _require_confirmation(payload)
    call_id, session_id = _capture_context(payload)
    doctor = _find_doctor(payload.get("doctor_name"), required=False)
    location = _find_location(payload.get("location_name"), doctor=doctor, required=False)
    reason, _requested_date = _appointment_request_summary(payload, doctor, location)
    mobile = _safe_text(payload.get("mobile"), "mobile", 40, required=True)
    caller_name = _safe_text(payload.get("caller_name"), "caller_name", 120)
    preferred_at = _preferred_datetime(payload.get("preferred_at"))

    def execute():
        try:
            row = create_callback(
                db.session,
                mobile=mobile,
                reason=reason,
                caller_name=caller_name,
                category="APPOINTMENT",
                priority=payload.get("priority", "NORMAL"),
                ai_summary="Appointment request received through OmniDimension.",
                preferred_at=preferred_at,
                call_id=call_id,
                session_id=session_id,
            )
        except OperationsError as exc:
            raise OmnidimGatewayError(exc.code, exc.message, exc.status_code)
        return (
            {
                "request_ref": row.callback_ref,
                "status": row.status,
                "staff_confirmation_required": True,
                "message": "The appointment request has been sent to clinic staff for confirmation.",
            },
            201,
            "CALLBACK",
            row.id,
        )

    result, status_code, replayed, row = _run_idempotent_action(
        "APPOINTMENT_REQUEST", payload, execute
    )
    result = dict(result)
    result["replayed"] = replayed
    _set_audit_action("OMNIDIM_APPOINTMENT_REQUEST", resource_type="CALLBACK", resource_id=row.resource_id)
    return _success(result, status_code)


@omnidim_gateway_bp.post("/callbacks")
def create_callback_request():
    _set_audit_action("OMNIDIM_CALLBACK_CREATE")
    if not bool(current_app.config.get("AI_CALLBACKS_ENABLED", False)):
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Callback intake is disabled", 503)
    payload = _json_object_body()
    _require_confirmation(payload)
    call_id, session_id = _capture_context(payload)
    mobile = _safe_text(payload.get("mobile"), "mobile", 40, required=True)
    reason = _safe_text(payload.get("reason"), "reason", 500, required=True)
    caller_name = _safe_text(payload.get("caller_name"), "caller_name", 120)
    category = _safe_text(payload.get("category"), "category", 80)
    ai_summary = _safe_text(payload.get("ai_summary"), "ai_summary", 1000)
    preferred_at = _preferred_datetime(payload.get("preferred_at"))

    def execute():
        try:
            row = create_callback(
                db.session,
                mobile=mobile,
                reason=reason,
                caller_name=caller_name,
                category=category,
                priority=payload.get("priority", "NORMAL"),
                ai_summary=ai_summary,
                preferred_at=preferred_at,
                call_id=call_id,
                session_id=session_id,
            )
        except OperationsError as exc:
            raise OmnidimGatewayError(exc.code, exc.message, exc.status_code)
        return (
            {"callback_ref": row.callback_ref, "status": row.status},
            201,
            "CALLBACK",
            row.id,
        )

    result, status_code, replayed, row = _run_idempotent_action("CALLBACK_CREATE", payload, execute)
    result = dict(result)
    result["replayed"] = replayed
    _set_audit_action("OMNIDIM_CALLBACK_CREATE", resource_type="CALLBACK", resource_id=row.resource_id)
    return _success(result, status_code)


@omnidim_gateway_bp.post("/complaints")
def create_complaint_request():
    _set_audit_action("OMNIDIM_COMPLAINT_CREATE")
    if not bool(current_app.config.get("AI_COMPLAINTS_ENABLED", False)):
        raise OmnidimGatewayError("SERVICE_UNAVAILABLE", "Complaint intake is disabled", 503)
    payload = _json_object_body()
    _require_confirmation(payload)
    call_id, session_id = _capture_context(payload)
    mobile = _safe_text(payload.get("mobile"), "mobile", 40, required=True)
    category = _safe_text(payload.get("category"), "category", 80, required=True)
    summary = _safe_text(payload.get("summary"), "summary", 240, required=True)
    details = _safe_text(payload.get("details"), "details", 4000)
    caller_name = _safe_text(payload.get("caller_name"), "caller_name", 120)

    def execute():
        try:
            row = create_complaint(
                db.session,
                mobile=mobile,
                category=category,
                summary=summary,
                details=details,
                priority=payload.get("priority", "NORMAL"),
                caller_name=caller_name,
                call_id=call_id,
                session_id=session_id,
            )
        except OperationsError as exc:
            raise OmnidimGatewayError(exc.code, exc.message, exc.status_code)
        return (
            {"complaint_ref": row.complaint_ref, "status": row.status},
            201,
            "COMPLAINT",
            row.id,
        )

    result, status_code, replayed, row = _run_idempotent_action("COMPLAINT_CREATE", payload, execute)
    result = dict(result)
    result["replayed"] = replayed
    _set_audit_action("OMNIDIM_COMPLAINT_CREATE", resource_type="COMPLAINT", resource_id=row.resource_id)
    return _success(result, status_code)
