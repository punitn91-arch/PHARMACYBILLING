"""Patient identification, OTP and verified-session endpoints."""

from flask import current_app, g, request

try:
    from ..models import db
    from ..services.ai_patient_service import (
        PatientAccessError,
        create_identification_intent,
        require_patient_session,
        send_verification_otp,
        verify_otp,
    )
    from .ai_api import (
        AIAPIError,
        ai_api_bp,
        enforce_api_rate_limit,
        json_object_body,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )
except ImportError:  # pragma: no cover
    from models import db
    from services.ai_patient_service import (
        PatientAccessError,
        create_identification_intent,
        require_patient_session,
        send_verification_otp,
        verify_otp,
    )
    from routes.ai_api import (
        AIAPIError,
        ai_api_bp,
        enforce_api_rate_limit,
        json_object_body,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )


def _translate_patient_error(exc):
    raise AIAPIError(exc.code, exc.message, exc.status_code)


def verified_patient_session(patient_id=None):
    raw_token = str(request.headers.get("X-Patient-Session") or "").strip()
    try:
        session = require_patient_session(
            raw_token,
            g.ai_principal.api_client_id,
            patient_id=patient_id,
        )
    except PatientAccessError as exc:
        _translate_patient_error(exc)
    g.ai_patient_session = session
    return session


@ai_api_bp.post("/patients/identify")
@require_ai_scope("patient:identify")
def identify_patient():
    set_ai_audit_action("PATIENT_IDENTIFY")
    enforce_api_rate_limit(20, window_seconds=60)
    payload = json_object_body()
    try:
        _intent, _raw_reference, result = create_identification_intent(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            mobile=payload.get("mobile"),
            claimed_name=payload.get("name"),
            call_id=getattr(g, "ai_call_id", None),
            session_id=getattr(g, "ai_session_id", None),
        )
        db.session.commit()
        return success_response(result, 202)
    except PatientAccessError as exc:
        db.session.rollback()
        _translate_patient_error(exc)


@ai_api_bp.post("/verification/otp/send")
@require_ai_scope("patient:verify")
def send_patient_verification_otp():
    set_ai_audit_action("OTP_SEND")
    if not bool(current_app.config.get("AI_OTP_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI patient OTP is disabled", 503)
    enforce_api_rate_limit(10, window_seconds=900)
    payload = json_object_body()
    try:
        result = send_verification_otp(
            db.session,
            reference=payload.get("identification_ref"),
            api_client_id=g.ai_principal.api_client_id,
            otp_expiry_seconds=current_app.config.get("OTP_EXPIRY_SECONDS", 300),
            max_attempts=current_app.config.get("OTP_MAX_ATTEMPTS", 5),
            resend_cooldown_seconds=current_app.config.get("OTP_RESEND_COOLDOWN_SECONDS", 60),
            is_production=bool(current_app.config.get("IS_PRODUCTION", False)),
            testing=bool(current_app.config.get("TESTING", False)),
        )
        return success_response(result, 202)
    except PatientAccessError as exc:
        db.session.rollback()
        _translate_patient_error(exc)


@ai_api_bp.post("/verification/otp/verify")
@require_ai_scope("patient:verify")
def verify_patient_otp():
    set_ai_audit_action("OTP_VERIFY")
    if not bool(current_app.config.get("AI_OTP_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI patient OTP is disabled", 503)
    enforce_api_rate_limit(20, window_seconds=900)
    payload = json_object_body()
    try:
        result = verify_otp(
            db.session,
            reference=payload.get("identification_ref"),
            otp_code=payload.get("otp"),
            api_client_id=g.ai_principal.api_client_id,
            session_ttl_seconds=current_app.config.get("AI_PATIENT_SESSION_TTL_SECONDS", 900),
        )
        set_ai_audit_action(
            "OTP_VERIFY", resource_type="PATIENT", resource_id=result["patient"]["id"]
        )
        return success_response(result)
    except PatientAccessError as exc:
        db.session.rollback()
        _translate_patient_error(exc)
