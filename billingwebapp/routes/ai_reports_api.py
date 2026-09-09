"""Verified report status and one-time secure document delivery."""

from flask import current_app, g, request, send_file
from datetime import datetime
from werkzeug.utils import secure_filename

try:
    from ..models import db
    from ..services.ai_report_service import (
        ReportAccessError,
        consume_document_token,
        create_secure_document_token,
        patient_reports,
    )
    from ..services.ai_notification_service import NotificationError, send_template_notification
    from .ai_api import (
        AIAPIError,
        AI_API_PREFIX,
        ai_api_bp,
        enforce_api_rate_limit,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )
    from .ai_patient_api import verified_patient_session
except ImportError:  # pragma: no cover
    from models import db
    from services.ai_report_service import (
        ReportAccessError,
        consume_document_token,
        create_secure_document_token,
        patient_reports,
    )
    from services.ai_notification_service import NotificationError, send_template_notification
    from routes.ai_api import (
        AIAPIError,
        AI_API_PREFIX,
        ai_api_bp,
        enforce_api_rate_limit,
        nonnegative_int,
        positive_int,
        require_ai_scope,
        set_ai_audit_action,
        success_response,
    )
    from routes.ai_patient_api import verified_patient_session


def _translate(exc):
    raise AIAPIError(exc.code, exc.message, exc.status_code)


def _optional_date(name):
    raw = str(request.args.get(name) or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise AIAPIError("INVALID_REQUEST", "{} must use YYYY-MM-DD".format(name), 400)


@ai_api_bp.get("/reports/status")
@require_ai_scope("report:read")
def get_report_status():
    set_ai_audit_action("REPORT_LOOKUP")
    enforce_api_rate_limit(20, window_seconds=60)
    patient_session = verified_patient_session()
    report_id = request.args.get("report_id")
    date_from = _optional_date("date_from")
    date_to = _optional_date("date_to")
    if date_from and date_to and date_from > date_to:
        raise AIAPIError("INVALID_REQUEST", "date_from must not be after date_to", 400)
    try:
        items = patient_reports(
            patient_session,
            report_id=positive_int(report_id, "report_id") if report_id else None,
            test_name=str(request.args.get("test_name") or "").strip() or None,
            date_from=date_from,
            date_to=date_to,
            limit=min(50, positive_int(request.args.get("limit", 20), "limit")),
            offset=nonnegative_int(request.args.get("offset", 0), "offset"),
        )
        return success_response(
            {
                "items": items,
                "count": len(items),
                "offset": nonnegative_int(request.args.get("offset", 0), "offset"),
            }
        )
    except ReportAccessError as exc:
        _translate(exc)


@ai_api_bp.post("/reports/<int:report_id>/secure-link")
@require_ai_scope("report:read")
def create_report_secure_link(report_id):
    set_ai_audit_action("REPORT_SECURE_LINK", resource_type="REPORT", resource_id=report_id)
    if not bool(current_app.config.get("AI_SECURE_REPORT_DELIVERY_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "Secure report delivery is disabled", 503)
    enforce_api_rate_limit(10, window_seconds=300)
    patient_session = verified_patient_session()
    try:
        raw_token, token, report = create_secure_document_token(
            db.session,
            patient_session=patient_session,
            report_id=report_id,
            expiry_minutes=current_app.config.get("SECURE_DOCUMENT_EXPIRY_MINUTES", 10),
        )
        base_url = str(current_app.config.get("APPLICATION_BASE_URL") or request.url_root).rstrip("/")
        if current_app.config.get("IS_PRODUCTION") and not base_url.startswith("https://"):
            db.session.rollback()
            raise AIAPIError("INTERNAL_ERROR", "Secure application URL is not configured", 500)
        db.session.commit()
        return success_response(
            {
                "report_id": report.id,
                "secure_url": "{}{}{}".format(base_url, AI_API_PREFIX, "/documents/{}".format(raw_token)),
                "expires_at": token.expires_at.isoformat() + "Z",
                "one_time": True,
            },
            201,
        )
    except ReportAccessError as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.post("/reports/<int:report_id>/send")
@require_ai_scope("report:send")
def send_report_link(report_id):
    set_ai_audit_action("REPORT_SEND", resource_type="REPORT", resource_id=report_id)
    if not bool(current_app.config.get("AI_SECURE_REPORT_DELIVERY_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "Secure report delivery is disabled", 503)
    if not bool(current_app.config.get("AI_NOTIFICATIONS_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "AI notifications are disabled", 503)
    enforce_api_rate_limit(10, window_seconds=300)
    payload = request.get_json(silent=True)
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise AIAPIError("INVALID_REQUEST", "A JSON object request body is required", 400)
    patient_session = verified_patient_session()
    try:
        raw_token, token, report = create_secure_document_token(
            db.session,
            patient_session=patient_session,
            report_id=report_id,
            expiry_minutes=current_app.config.get("SECURE_DOCUMENT_EXPIRY_MINUTES", 10),
        )
        base_url = str(current_app.config.get("APPLICATION_BASE_URL") or request.url_root).rstrip("/")
        if current_app.config.get("IS_PRODUCTION") and not base_url.startswith("https://"):
            raise AIAPIError("INTERNAL_ERROR", "Secure application URL is not configured", 500)
        secure_url = "{}{}{}".format(base_url, AI_API_PREFIX, "/documents/{}".format(raw_token))
        delivery, duplicate = send_template_notification(
            db.session,
            api_client_id=g.ai_principal.api_client_id,
            event_code="REPORT_LINK",
            channel=payload.get("channel", "SMS"),
            language=payload.get("language", "en"),
            recipient=patient_session.mobile,
            variables={"report_title": report.title, "secure_link": secure_url},
            idempotency_key=request.headers.get("Idempotency-Key"),
            fingerprint_secret=current_app.config.get("AI_AUDIT_FINGERPRINT_SECRET"),
            testing=bool(current_app.config.get("TESTING")),
        )
        if duplicate:
            db.session.delete(token)
        else:
            report.delivery_status = "SENT"
            report.last_delivery_at = datetime.utcnow()
        db.session.commit()
        set_ai_audit_action("REPORT_SEND", resource_type="REPORT", resource_id=report.id)
        return success_response(
            {
                "report_id": report.id,
                "delivery_id": delivery.id,
                "status": delivery.status,
                "duplicate": duplicate,
                "link_expires_at": token.expires_at.isoformat() + "Z" if not duplicate else None,
            },
            200 if duplicate else 202,
        )
    except (ReportAccessError, NotificationError) as exc:
        db.session.rollback()
        _translate(exc)


@ai_api_bp.get("/documents/<raw_token>")
def download_secure_document(raw_token):
    set_ai_audit_action("DOCUMENT_DOWNLOAD")
    if not bool(current_app.config.get("AI_SECURE_REPORT_DELIVERY_ENABLED", False)):
        raise AIAPIError("SERVICE_UNAVAILABLE", "Secure report delivery is disabled", 503)
    try:
        report, file_path = consume_document_token(
            db.session,
            raw_token,
            private_storage_root=current_app.config["APP_PRIVATE_STORAGE_ROOT"],
        )
        set_ai_audit_action("DOCUMENT_DOWNLOAD", resource_type="REPORT", resource_id=report.id)
        download_name = secure_filename(report.original_filename or "lab-report.pdf") or "lab-report.pdf"
        response = send_file(
            file_path,
            as_attachment=True,
            download_name=download_name,
            mimetype=report.mime_type or "application/pdf",
            conditional=True,
        )
        response.headers["Cache-Control"] = "private, no-store, max-age=0"
        response.headers["Content-Security-Policy"] = "default-src 'none'"
        return response
    except ReportAccessError as exc:
        db.session.rollback()
        _translate(exc)
