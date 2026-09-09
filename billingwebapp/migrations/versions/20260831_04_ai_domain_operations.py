"""Add complete AI clinic domain, verification, operations and delivery schema.

Revision ID: 20260831_04
Revises: 20260831_03
Create Date: 2026-08-31
"""

from alembic import op
import sqlalchemy as sa


revision = "20260831_04"
down_revision = "20260831_03"
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
    existing = _columns(table)
    for column in definitions:
        if column.name not in existing:
            op.add_column(table, column)


def _add_index(table, name, columns, unique=False):
    if name not in _indexes(table):
        op.create_index(name, table, columns, unique=unique)


def _extend_existing_tables():
    _add_columns("ai_api_request_audit", [
        sa.Column("action", sa.String(80), nullable=True),
        sa.Column("resource_type", sa.String(50), nullable=True),
        sa.Column("resource_id", sa.String(80), nullable=True),
    ])
    _add_index("ai_api_request_audit", "ix_ai_api_request_audit_action", ["action"])
    _add_index("ai_api_request_audit", "ix_ai_api_request_audit_resource_type", ["resource_type"])
    _add_index("ai_api_request_audit", "ix_ai_api_request_audit_resource_id", ["resource_id"])

    if _has_table("clinic_profile"):
        _add_columns("clinic_profile", [
            sa.Column("late_grace_minutes", sa.Integer(), nullable=False, server_default="15"),
            sa.Column("auto_reschedule_after_minutes", sa.Integer(), nullable=True),
            sa.Column("late_staff_notification_required", sa.Boolean(), nullable=False, server_default=sa.true()),
        ])
    if _has_table("patient_verification_intent"):
        _add_columns("patient_verification_intent", [
            sa.Column("claimed_name", sa.String(120), nullable=True),
        ])

    _add_columns("appointment", [
        sa.Column("clinician_id", sa.Integer(), nullable=True),
        sa.Column("location_id", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(30), nullable=False, server_default="ADMIN"),
        sa.Column("external_call_id", sa.String(128), nullable=True),
        sa.Column("external_session_id", sa.String(128), nullable=True),
        sa.Column("external_request_id", sa.String(80), nullable=True),
        sa.Column("slot_start_at", sa.DateTime(), nullable=True),
        sa.Column("slot_end_at", sa.DateTime(), nullable=True),
        sa.Column("idempotency_key_hash", sa.String(64), nullable=True),
        sa.Column("cancellation_reason", sa.String(255), nullable=True),
        sa.Column("cancelled_by_source", sa.String(30), nullable=True),
        sa.Column("rescheduled_from_id", sa.Integer(), nullable=True),
        sa.Column("previous_appointment_date", sa.Date(), nullable=True),
        sa.Column("previous_appointment_time", sa.Time(), nullable=True),
        sa.Column("rescheduled_at", sa.DateTime(), nullable=True),
        sa.Column("late_arrival_status", sa.String(30), nullable=True),
        sa.Column("late_arrival_at", sa.DateTime(), nullable=True),
    ])
    for name, columns in (
        ("ix_appointment_clinician_date_status", ["clinician_id", "appointment_date", "status"]),
        ("ix_appointment_location_date", ["location_id", "appointment_date"]),
        ("ix_appointment_slot_start_at", ["slot_start_at"]),
        ("ix_appointment_source", ["source"]),
        ("ix_appointment_external_call_id", ["external_call_id"]),
        ("ix_appointment_rescheduled_at", ["rescheduled_at"]),
    ):
        _add_index("appointment", name, columns)

    _add_columns("lab_report", [
        sa.Column("delivery_status", sa.String(30), nullable=False, server_default="NOT_SENT"),
        sa.Column("last_delivery_at", sa.DateTime(), nullable=True),
    ])
    _add_index("lab_report", "ix_lab_report_delivery_status", ["delivery_status"])

    _add_columns("clinic_location", [
        sa.Column("location_type", sa.String(30), nullable=False, server_default="CLINIC"),
        sa.Column("landmark", sa.String(160), nullable=True),
        sa.Column("city", sa.String(100), nullable=True),
        sa.Column("state", sa.String(100), nullable=True),
        sa.Column("postal_code", sa.String(12), nullable=True),
        sa.Column("latitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("longitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("maps_url", sa.String(500), nullable=True),
        sa.Column("parking_information", sa.String(500), nullable=True),
    ])
    _add_index("clinic_location", "ix_clinic_location_location_type", ["location_type"])
    _add_index("clinic_location", "ix_clinic_location_city", ["city"])

    _add_columns("clinician", [
        sa.Column("qualification", sa.String(180), nullable=True),
        sa.Column("consultation_fee", sa.Numeric(10, 2), nullable=True),
        sa.Column("follow_up_fee", sa.Numeric(10, 2), nullable=True),
        sa.Column("follow_up_days", sa.Integer(), nullable=True),
        sa.Column("public_bio", sa.String(500), nullable=True),
    ])
    _add_columns("clinic_schedule_rule", [
        sa.Column("slot_duration_minutes", sa.Integer(), nullable=False, server_default="20"),
        sa.Column("max_patients_per_slot", sa.Integer(), nullable=False, server_default="1"),
    ])
    _add_columns("clinic_schedule_exception", [
        sa.Column("slot_duration_minutes", sa.Integer(), nullable=True),
        sa.Column("max_patients_per_slot", sa.Integer(), nullable=True),
    ])


def _create_domain_tables():
    if not _has_table("clinic_profile"):
        op.create_table(
            "clinic_profile",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("clinic_name", sa.String(160), nullable=False),
            sa.Column("phone", sa.String(30)), sa.Column("email", sa.String(160)),
            sa.Column("reception_phone", sa.String(30)), sa.Column("website_url", sa.String(500)),
            sa.Column("emergency_wording", sa.String(500)),
            sa.Column("consultation_information", sa.Text()), sa.Column("general_policies", sa.Text()),
            sa.Column("payment_methods_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column("available_services_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column("timezone_name", sa.String(64), nullable=False, server_default="Asia/Kolkata"),
            sa.Column("late_grace_minutes", sa.Integer(), nullable=False, server_default="15"),
            sa.Column("auto_reschedule_after_minutes", sa.Integer()),
            sa.Column("late_staff_notification_required", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("updated_by", sa.String(50)),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_clinic_profile_is_active", "clinic_profile", ["is_active"])

    if not _has_table("appointment_slot_lock"):
        op.create_table(
            "appointment_slot_lock",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("clinician_id", sa.Integer(), nullable=False),
            sa.Column("location_id", sa.Integer(), nullable=False),
            sa.Column("appointment_date", sa.Date(), nullable=False),
            sa.Column("slot_time", sa.Time(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("clinician_id", "location_id", "appointment_date", "slot_time", name="uq_appointment_slot_lock_identity"),
        )
        op.create_index("ix_appointment_slot_lock_lookup", "appointment_slot_lock", ["clinician_id", "location_id", "appointment_date", "slot_time"])

    if not _has_table("appointment_slot_block"):
        op.create_table(
            "appointment_slot_block",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("clinician_id", sa.Integer(), sa.ForeignKey("clinician.id")),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("clinic_location.id")),
            sa.Column("block_date", sa.Date(), nullable=False),
            sa.Column("start_time", sa.Time()), sa.Column("end_time", sa.Time()),
            sa.Column("all_day", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("public_reason", sa.String(240)),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(50)), sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_appointment_slot_block_lookup", "appointment_slot_block", ["clinician_id", "location_id", "block_date", "is_active"])

    if not _has_table("appointment_waitlist"):
        op.create_table(
            "appointment_waitlist",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("waitlist_ref", sa.String(40), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id"), nullable=False),
            sa.Column("clinician_id", sa.Integer(), sa.ForeignKey("clinician.id"), nullable=False),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("clinic_location.id"), nullable=False),
            sa.Column("preferred_date", sa.Date(), nullable=False),
            sa.Column("preferred_start_time", sa.Time()), sa.Column("preferred_end_time", sa.Time()),
            sa.Column("status", sa.String(30), nullable=False, server_default="WAITING"),
            sa.Column("source", sa.String(30), nullable=False, server_default="AI_CALL"),
            sa.Column("external_call_id", sa.String(128)), sa.Column("external_session_id", sa.String(128)),
            sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_appointment_waitlist_ref", "appointment_waitlist", ["waitlist_ref"], unique=True)
        op.create_index("ix_appointment_waitlist_lookup", "appointment_waitlist", ["clinician_id", "location_id", "preferred_date", "status"])

    if not _has_table("patient_verification_intent"):
        op.create_table(
            "patient_verification_intent",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("reference_hash", sa.String(64), nullable=False),
            sa.Column("api_client_id", sa.Integer(), sa.ForeignKey("ai_api_client.id"), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id")),
            sa.Column("mobile", sa.String(20), nullable=False),
            sa.Column("claimed_name", sa.String(120)),
            sa.Column("external_call_id", sa.String(128)), sa.Column("external_session_id", sa.String(128)),
            sa.Column("expires_at", sa.DateTime(), nullable=False), sa.Column("used_at", sa.DateTime()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_patient_verification_intent_reference_hash", "patient_verification_intent", ["reference_hash"], unique=True)
        op.create_index("ix_patient_verification_intent_client_expiry", "patient_verification_intent", ["api_client_id", "expires_at"])
        op.create_index("ix_patient_verification_intent_mobile", "patient_verification_intent", ["mobile"])

    if not _has_table("patient_verification_session"):
        op.create_table(
            "patient_verification_session",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("token_hash", sa.String(64), nullable=False),
            sa.Column("api_client_id", sa.Integer(), sa.ForeignKey("ai_api_client.id"), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id"), nullable=False),
            sa.Column("mobile", sa.String(20), nullable=False),
            sa.Column("purpose", sa.String(40), nullable=False, server_default="AI_PATIENT_ACCESS"),
            sa.Column("otp_challenge_id", sa.Integer(), sa.ForeignKey("portal_otp_challenge.id")),
            sa.Column("external_call_id", sa.String(128)), sa.Column("external_session_id", sa.String(128)),
            sa.Column("expires_at", sa.DateTime(), nullable=False), sa.Column("last_used_at", sa.DateTime()),
            sa.Column("revoked_at", sa.DateTime()), sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_patient_verification_session_token_hash", "patient_verification_session", ["token_hash"], unique=True)
        op.create_index("ix_patient_verification_session_client_patient", "patient_verification_session", ["api_client_id", "patient_id", "expires_at"])

    if not _has_table("secure_document_token"):
        op.create_table(
            "secure_document_token",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("token_hash", sa.String(64), nullable=False),
            sa.Column("report_id", sa.Integer(), sa.ForeignKey("lab_report.id"), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id"), nullable=False),
            sa.Column("verification_session_id", sa.Integer(), sa.ForeignKey("patient_verification_session.id"), nullable=False),
            sa.Column("expires_at", sa.DateTime(), nullable=False),
            sa.Column("max_downloads", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("download_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_downloaded_at", sa.DateTime()), sa.Column("revoked_at", sa.DateTime()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_secure_document_token_hash", "secure_document_token", ["token_hash"], unique=True)
        op.create_index("ix_secure_document_token_report_expiry", "secure_document_token", ["report_id", "expires_at"])

    if not _has_table("clinic_knowledge_entry"):
        op.create_table(
            "clinic_knowledge_entry",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("category", sa.String(80), nullable=False), sa.Column("question", sa.String(240), nullable=False),
            sa.Column("answer", sa.Text(), nullable=False), sa.Column("language", sa.String(12), nullable=False, server_default="en"),
            sa.Column("keywords_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(50)), sa.Column("updated_by", sa.String(50)),
            sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_clinic_knowledge_category_language", "clinic_knowledge_entry", ["category", "language", "is_active"])

    if not _has_table("reception_schedule"):
        op.create_table(
            "reception_schedule",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("clinic_location.id"), nullable=False),
            sa.Column("weekday", sa.Integer(), nullable=False), sa.Column("is_open", sa.Boolean(), nullable=False),
            sa.Column("open_time", sa.Time()), sa.Column("close_time", sa.Time()), sa.Column("public_note", sa.String(240)),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(50)), sa.Column("updated_by", sa.String(50)),
            sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_reception_schedule_lookup", "reception_schedule", ["location_id", "weekday", "is_active"])

    if not _has_table("reception_schedule_override"):
        op.create_table(
            "reception_schedule_override",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("clinic_location.id"), nullable=False),
            sa.Column("schedule_date", sa.Date(), nullable=False), sa.Column("is_open", sa.Boolean(), nullable=False),
            sa.Column("open_time", sa.Time()), sa.Column("close_time", sa.Time()), sa.Column("public_note", sa.String(240)),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(50)), sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_reception_override_lookup", "reception_schedule_override", ["location_id", "schedule_date", "is_active"])

    if not _has_table("callback_request"):
        op.create_table(
            "callback_request",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("callback_ref", sa.String(40), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id")), sa.Column("caller_name", sa.String(120)),
            sa.Column("mobile", sa.String(20), nullable=False), sa.Column("reason", sa.String(500), nullable=False),
            sa.Column("category", sa.String(80)),
            sa.Column("priority", sa.String(20), nullable=False, server_default="NORMAL"),
            sa.Column("ai_summary", sa.String(1000)),
            sa.Column("preferred_at", sa.DateTime()), sa.Column("status", sa.String(30), nullable=False, server_default="PENDING"),
            sa.Column("source", sa.String(30), nullable=False, server_default="AI_CALL"),
            sa.Column("external_call_id", sa.String(128)), sa.Column("external_session_id", sa.String(128)),
            sa.Column("assigned_to", sa.String(50)), sa.Column("resolution_note", sa.String(500)),
            sa.Column("contacted_at", sa.DateTime()),
            sa.Column("completed_at", sa.DateTime()), sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_callback_request_ref", "callback_request", ["callback_ref"], unique=True)
        op.create_index("ix_callback_request_status_created", "callback_request", ["status", "created_at"])
        op.create_index("ix_callback_request_mobile", "callback_request", ["mobile"])
        op.create_index("ix_callback_request_priority", "callback_request", ["priority"])
        op.create_index("ix_callback_request_category", "callback_request", ["category"])

    if not _has_table("complaint"):
        op.create_table(
            "complaint",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("complaint_ref", sa.String(40), nullable=False),
            sa.Column("patient_id", sa.Integer(), sa.ForeignKey("patient.id")), sa.Column("caller_name", sa.String(120)),
            sa.Column("mobile", sa.String(20), nullable=False), sa.Column("category", sa.String(80), nullable=False),
            sa.Column("summary", sa.String(240), nullable=False), sa.Column("details", sa.Text()),
            sa.Column("priority", sa.String(20), nullable=False, server_default="NORMAL"),
            sa.Column("status", sa.String(30), nullable=False, server_default="OPEN"),
            sa.Column("source", sa.String(30), nullable=False, server_default="AI_CALL"),
            sa.Column("external_call_id", sa.String(128)), sa.Column("external_session_id", sa.String(128)),
            sa.Column("assigned_to", sa.String(50)), sa.Column("resolution_note", sa.String(1000)),
            sa.Column("resolved_at", sa.DateTime()), sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_complaint_ref", "complaint", ["complaint_ref"], unique=True)
        op.create_index("ix_complaint_status_priority", "complaint", ["status", "priority", "created_at"])
        op.create_index("ix_complaint_mobile", "complaint", ["mobile"])

    if not _has_table("notification_template"):
        op.create_table(
            "notification_template",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("event_code", sa.String(80), nullable=False),
            sa.Column("channel", sa.String(20), nullable=False), sa.Column("language", sa.String(12), nullable=False, server_default="en"),
            sa.Column("body_template", sa.Text(), nullable=False), sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(50)), sa.Column("updated_by", sa.String(50)),
            sa.Column("created_at", sa.DateTime(), nullable=False), sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("event_code", "channel", "language", name="uq_notification_template_identity"),
        )
        op.create_index("ix_notification_template_lookup", "notification_template", ["event_code", "channel", "language", "is_active"])

    if not _has_table("notification_delivery"):
        op.create_table(
            "notification_delivery",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("api_client_id", sa.Integer(), sa.ForeignKey("ai_api_client.id"), nullable=False),
            sa.Column("template_id", sa.Integer(), sa.ForeignKey("notification_template.id"), nullable=False),
            sa.Column("channel", sa.String(20), nullable=False), sa.Column("recipient_masked", sa.String(30), nullable=False),
            sa.Column("recipient_fingerprint", sa.String(64), nullable=False),
            sa.Column("idempotency_key_hash", sa.String(64), nullable=False),
            sa.Column("request_fingerprint", sa.String(64), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="PENDING"),
            sa.Column("provider", sa.String(40)), sa.Column("provider_reference", sa.String(120)),
            sa.Column("error_code", sa.String(80)), sa.Column("sent_at", sa.DateTime()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("api_client_id", "idempotency_key_hash", name="uq_notification_delivery_client_idempotency"),
        )
        op.create_index("ix_notification_delivery_status_created", "notification_delivery", ["status", "created_at"])
        op.create_index("ix_notification_delivery_recipient_fingerprint", "notification_delivery", ["recipient_fingerprint"])


def upgrade():
    _extend_existing_tables()
    _create_domain_tables()


def downgrade():
    for table in (
        "notification_delivery", "notification_template", "complaint", "callback_request",
        "reception_schedule_override", "reception_schedule", "clinic_knowledge_entry",
        "secure_document_token", "patient_verification_session", "patient_verification_intent",
        "appointment_waitlist", "appointment_slot_block", "appointment_slot_lock", "clinic_profile",
    ):
        if _has_table(table):
            op.drop_table(table)
    # Existing-table columns are retained on downgrade to keep legacy clinic
    # and appointment data recoverable. This downgrade is intentionally
    # non-destructive; application rollback simply stops using those columns.
