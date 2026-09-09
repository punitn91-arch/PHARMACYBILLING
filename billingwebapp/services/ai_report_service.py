"""Patient-owned report status and expiring document-token operations."""

from datetime import datetime, timedelta
import hashlib
import os
import secrets

from werkzeug.utils import secure_filename

try:
    from ..models import db, LabOrder, LabReport, SecureDocumentToken
except ImportError:  # pragma: no cover
    from models import db, LabOrder, LabReport, SecureDocumentToken


class ReportAccessError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _hash(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def standard_report_status(report):
    status = str(report.status or "").upper()
    if status == "PUBLISHED":
        return "READY"
    if status == "REVOKED":
        return "UNAVAILABLE"
    return "PROCESSING"


def patient_reports(
    patient_session,
    *,
    report_id=None,
    test_name=None,
    date_from=None,
    date_to=None,
    limit=20,
    offset=0,
):
    query = LabReport.query.filter(LabReport.patient_id == patient_session.patient_id)
    if report_id is not None:
        query = query.filter(LabReport.id == int(report_id))
    if test_name:
        query = query.filter(LabReport.title.ilike("%{}%".format(str(test_name)[:120])))
    if date_from:
        query = query.filter(LabReport.report_date >= date_from)
    if date_to:
        query = query.filter(LabReport.report_date <= date_to)
    rows = query.order_by(
        LabReport.report_date.desc(), LabReport.uploaded_at.desc(), LabReport.id.desc()
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 50))).all()
    order_ids = {row.lab_order_id for row in rows}
    order_numbers = dict(
        db.session.query(LabOrder.id, LabOrder.order_no).filter(LabOrder.id.in_(order_ids)).all()
    ) if order_ids else {}
    return [
        {
            "report_id": row.id,
            "order_no": order_numbers.get(row.lab_order_id),
            "title": row.title,
            "report_date": row.report_date.isoformat() if row.report_date else None,
            "status": standard_report_status(row),
            "delivery_status": row.delivery_status or "NOT_SENT",
        }
        for row in rows
    ]


def owned_ready_report(patient_session, report_id):
    report = LabReport.query.filter_by(
        id=int(report_id),
        patient_id=patient_session.patient_id,
    ).first()
    if not report:
        raise ReportAccessError("REPORT_NOT_FOUND", "Report was not found", 404)
    if standard_report_status(report) != "READY":
        raise ReportAccessError("REPORT_NOT_READY", "Report is not ready for download", 409)
    return report


def create_secure_document_token(
    db_session,
    *,
    patient_session,
    report_id,
    expiry_minutes=10,
    now=None,
):
    now = now or datetime.utcnow()
    report = owned_ready_report(patient_session, report_id)
    raw_token = "doc_{}".format(secrets.token_urlsafe(48))
    expiry = max(1, min(int(expiry_minutes), 60))
    token = SecureDocumentToken(
        token_hash=_hash(raw_token),
        report_id=report.id,
        patient_id=patient_session.patient_id,
        verification_session_id=patient_session.id,
        expires_at=now + timedelta(minutes=expiry),
        max_downloads=1,
        download_count=0,
        created_at=now,
    )
    db_session.add(token)
    db_session.flush()
    return raw_token, token, report


def consume_document_token(db_session, raw_token, *, private_storage_root, now=None):
    now = now or datetime.utcnow()
    token = SecureDocumentToken.query.filter_by(token_hash=_hash(raw_token)).with_for_update().first()
    if (
        not token
        or token.revoked_at is not None
        or token.expires_at <= now
        or int(token.download_count or 0) >= int(token.max_downloads or 1)
    ):
        raise ReportAccessError("REPORT_ACCESS_DENIED", "Document link is invalid or expired", 403)
    report = LabReport.query.filter_by(
        id=token.report_id,
        patient_id=token.patient_id,
        status="PUBLISHED",
    ).first()
    if not report:
        raise ReportAccessError("REPORT_ACCESS_DENIED", "Document link is invalid or expired", 403)
    storage_key = secure_filename(report.storage_key or "")
    if not storage_key or storage_key != report.storage_key:
        raise ReportAccessError("REPORT_NOT_FOUND", "Report file is unavailable", 404)
    report_dir = os.path.join(os.path.abspath(private_storage_root), "lab_reports")
    file_path = os.path.abspath(os.path.join(report_dir, storage_key))
    if not file_path.startswith(report_dir + os.sep) or not os.path.isfile(file_path):
        raise ReportAccessError("REPORT_NOT_FOUND", "Report file is unavailable", 404)
    token.download_count = int(token.download_count or 0) + 1
    token.last_downloaded_at = now
    report.download_count = int(report.download_count or 0) + 1
    report.last_downloaded_at = now
    db_session.commit()
    return report, file_path
