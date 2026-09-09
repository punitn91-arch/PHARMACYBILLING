"""Add final AI columns when draft tables already existed.

Revision ID: 20260831_05
Revises: 20260831_04

This is strictly additive. Draft columns and rows are never removed.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260831_05"
down_revision = "20260831_04"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _has_table(name):
    return name in _inspector().get_table_names()


def _columns(table):
    return {column["name"] for column in _inspector().get_columns(table)}


def _indexes(table):
    return {index["name"] for index in _inspector().get_indexes(table)}


def _add_columns(table, definitions):
    if not _has_table(table):
        return
    existing = _columns(table)
    for column in definitions:
        if column.name not in existing:
            op.add_column(table, column)


def _add_index(table, name, columns):
    if _has_table(table) and name not in _indexes(table) and set(columns).issubset(_columns(table)):
        op.create_index(name, table, columns, unique=False)


def upgrade():
    _add_columns("ai_api_client", [
        sa.Column("secret_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("updated_by", sa.String(50)),
    ])
    _add_columns("ai_api_request_audit", [
        sa.Column("outcome", sa.String(20), nullable=False, server_default="UNKNOWN"),
        sa.Column("client_ip_fingerprint", sa.String(64)),
    ])
    _add_columns("ai_idempotency_record", [
        sa.Column("idempotency_key_hash", sa.String(64)),
        sa.Column("state", sa.String(20), nullable=False, server_default="IN_PROGRESS"),
        sa.Column("response_status", sa.Integer()),
        sa.Column("updated_at", sa.DateTime()),
    ])
    _add_columns("clinic_profile", [
        sa.Column("phone", sa.String(30)),
        sa.Column("email", sa.String(160)),
        sa.Column("website_url", sa.String(500)),
        sa.Column("emergency_wording", sa.String(500)),
        sa.Column("general_policies", sa.Text()),
        sa.Column("available_services_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("timezone_name", sa.String(64), nullable=False, server_default="Asia/Kolkata"),
    ])
    _add_columns("appointment_slot_lock", [sa.Column("slot_time", sa.Time())])
    _add_columns("appointment_slot_block", [
        sa.Column("block_date", sa.Date()),
        sa.Column("all_day", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("public_reason", sa.String(240)),
    ])
    _add_columns("appointment_waitlist", [
        sa.Column("waitlist_ref", sa.String(40)),
        sa.Column("external_session_id", sa.String(128)),
    ])
    _add_columns("patient_verification_session", [
        sa.Column("mobile", sa.String(20)),
        sa.Column("last_used_at", sa.DateTime()),
    ])
    _add_columns("secure_document_token", [
        sa.Column("report_id", sa.Integer()),
        sa.Column("max_downloads", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("download_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_downloaded_at", sa.DateTime()),
    ])
    _add_columns("clinic_knowledge_entry", [
        sa.Column("question", sa.String(240)),
        sa.Column("answer", sa.Text()),
        sa.Column("language", sa.String(12), nullable=False, server_default="en"),
        sa.Column("keywords_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("created_by", sa.String(50)),
    ])
    _add_columns("reception_schedule_override", [
        sa.Column("is_open", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("open_time", sa.Time()),
        sa.Column("close_time", sa.Time()),
    ])
    _add_columns("callback_request", [
        sa.Column("callback_ref", sa.String(40)),
        sa.Column("mobile", sa.String(20)),
        sa.Column("preferred_at", sa.DateTime()),
        sa.Column("assigned_to", sa.String(50)),
        sa.Column("resolution_note", sa.String(500)),
    ])
    _add_columns("complaint", [
        sa.Column("complaint_ref", sa.String(40)),
        sa.Column("caller_name", sa.String(120)),
        sa.Column("mobile", sa.String(20)),
        sa.Column("summary", sa.String(240)),
        sa.Column("details", sa.Text()),
        sa.Column("assigned_to", sa.String(50)),
        sa.Column("resolution_note", sa.String(1000)),
    ])
    _add_columns("notification_delivery", [sa.Column("request_fingerprint", sa.String(64))])
    _add_columns("lab_report", [
        sa.Column("patient_note", sa.String(500)),
        sa.Column("report_date", sa.Date()),
    ])

    for table, name, columns in (
        ("ai_api_client", "ix_ai_api_client_secret_version", ["secret_version"]),
        ("ai_api_request_audit", "ix_ai_api_request_audit_outcome", ["outcome"]),
        ("ai_api_request_audit", "ix_ai_api_request_audit_client_ip_fingerprint", ["client_ip_fingerprint"]),
        ("ai_idempotency_record", "ix_ai_idempotency_record_idempotency_key_hash", ["idempotency_key_hash"]),
        ("ai_idempotency_record", "ix_ai_idempotency_record_state", ["state"]),
        ("appointment_slot_lock", "ix_appointment_slot_lock_lookup", ["clinician_id", "location_id", "appointment_date", "slot_time"]),
        ("appointment_slot_block", "ix_appointment_slot_block_lookup", ["clinician_id", "location_id", "block_date", "is_active"]),
        ("appointment_waitlist", "ix_appointment_waitlist_ref", ["waitlist_ref"]),
        ("patient_verification_session", "ix_patient_verification_session_mobile", ["mobile"]),
        ("secure_document_token", "ix_secure_document_token_report_expiry", ["report_id", "expires_at"]),
        ("clinic_knowledge_entry", "ix_clinic_knowledge_category_language", ["category", "language", "is_active"]),
        ("callback_request", "ix_callback_request_ref", ["callback_ref"]),
        ("callback_request", "ix_callback_request_mobile", ["mobile"]),
        ("complaint", "ix_complaint_ref", ["complaint_ref"]),
        ("complaint", "ix_complaint_mobile", ["mobile"]),
        ("lab_report", "ix_lab_report_report_date", ["report_date"]),
    ):
        _add_index(table, name, columns)


def downgrade():
    # Non-destructive by design: either draft or final columns may hold data.
    pass
