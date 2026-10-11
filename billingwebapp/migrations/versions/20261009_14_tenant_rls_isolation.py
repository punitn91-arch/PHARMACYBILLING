"""Tenant isolation indexes, backfill and PostgreSQL RLS policies.

Revision ID: 20261009_14
Revises: 20261009_13
Create Date: 2026-10-09
"""

from alembic import op
import sqlalchemy as sa


revision = "20261009_14"
down_revision = "20261009_13"
branch_labels = None
depends_on = None


TENANT_TABLES = (
    "user",
    "medicine",
    "invoice",
    "invoice_item",
    "return_bill",
    "return_item",
    "return_lot_allocation",
    "quarantine_stock",
    "hold_bill",
    "pending_bill_store",
    "patient",
    "appointment",
    "lab_test",
    "lab_order",
    "lab_order_item",
    "lab_report",
    "portal_otp_challenge",
    "appointment_booking_settings",
    "public_appointment_booking",
    "public_appointment_day_lock",
    "clinic_location",
    "clinician",
    "clinic_schedule_rule",
    "clinic_schedule_exception",
    "stock_history",
    "vendor",
    "vendor_purchase",
    "vendor_purchase_item",
    "vendor_notes",
    "vendor_note_items",
    "vendor_note_allocations",
    "vendor_ledger",
    "sales_allocation",
    "audit_log",
    "login_security_event",
    "voice_call",
    "voice_call_event",
    "ai_api_client",
    "ai_access_token",
    "ai_api_request_audit",
    "ai_idempotency_record",
    "omnidim_gateway_action",
    "clinic_profile",
    "appointment_slot_lock",
    "appointment_slot_block",
    "appointment_waitlist",
    "patient_verification_session",
    "patient_verification_intent",
    "secure_document_token",
    "clinic_knowledge_entry",
    "reception_schedule",
    "reception_schedule_override",
    "callback_request",
    "complaint",
    "notification_template",
    "notification_delivery",
)


PARENT_BACKFILLS = (
    ("invoice_item", "invoice_id", "invoice"),
    ("return_bill", "invoice_id", "invoice"),
    ("return_item", "return_id", "return_bill"),
    ("return_lot_allocation", "return_item_id", "return_item"),
    ("quarantine_stock", "return_id", "return_bill"),
    ("pending_bill_store", "legacy_hold_bill_id", "hold_bill"),
    ("appointment", "patient_id", "patient"),
    ("lab_order", "patient_id", "patient"),
    ("lab_order_item", "lab_order_id", "lab_order"),
    ("lab_report", "lab_order_id", "lab_order"),
    ("public_appointment_booking", "appointment_id", "appointment"),
    ("clinic_schedule_rule", "location_id", "clinic_location"),
    ("clinic_schedule_exception", "location_id", "clinic_location"),
    ("clinician", "default_location_id", "clinic_location"),
    ("vendor_purchase", "vendor_id", "vendor"),
    ("vendor_purchase_item", "purchase_id", "vendor_purchase"),
    ("vendor_notes", "vendor_id", "vendor"),
    ("vendor_note_items", "note_id", "vendor_notes"),
    ("vendor_note_allocations", "note_id", "vendor_notes"),
    ("vendor_ledger", "vendor_id", "vendor"),
    ("sales_allocation", "invoice_item_id", "invoice_item"),
    ("voice_call_event", "voice_call_id", "voice_call"),
    ("ai_access_token", "api_client_id", "ai_api_client"),
    ("ai_api_request_audit", "api_client_id", "ai_api_client"),
    ("ai_idempotency_record", "api_client_id", "ai_api_client"),
    ("appointment_slot_block", "location_id", "clinic_location"),
    ("appointment_waitlist", "patient_id", "patient"),
    ("patient_verification_session", "patient_id", "patient"),
    ("patient_verification_intent", "patient_id", "patient"),
    ("secure_document_token", "patient_id", "patient"),
    ("reception_schedule", "location_id", "clinic_location"),
    ("reception_schedule_override", "location_id", "clinic_location"),
    ("callback_request", "patient_id", "patient"),
    ("complaint", "patient_id", "patient"),
    ("notification_delivery", "template_id", "notification_template"),
)


TENANT_UNIQUE_INDEXES = (
    ("uq_user_tenant_username_ci", "user", ("tenant_id", "lower(username)"), "tenant_id IS NOT NULL AND username IS NOT NULL"),
    ("uq_invoice_tenant_invoice_no", "invoice", ("tenant_id", "invoice_no"), "tenant_id IS NOT NULL AND invoice_no IS NOT NULL"),
    ("uq_return_tenant_return_no", "return_bill", ("tenant_id", "return_no"), "tenant_id IS NOT NULL AND return_no IS NOT NULL"),
    ("uq_vendor_purchase_tenant_purchase_no", "vendor_purchase", ("tenant_id", "purchase_no"), "tenant_id IS NOT NULL AND purchase_no IS NOT NULL"),
    ("uq_lab_test_tenant_test_code", "lab_test", ("tenant_id", "test_code"), "tenant_id IS NOT NULL AND test_code IS NOT NULL"),
    ("uq_lab_order_tenant_order_no", "lab_order", ("tenant_id", "order_no"), "tenant_id IS NOT NULL AND order_no IS NOT NULL"),
    ("uq_appointment_tenant_appointment_no", "appointment", ("tenant_id", "appointment_no"), "tenant_id IS NOT NULL AND appointment_no IS NOT NULL"),
)


def _inspector():
    return sa.inspect(op.get_bind())


def _dialect():
    return op.get_bind().dialect.name.lower()


def _has_table(name):
    return name in _inspector().get_table_names()


def _columns(table):
    if not _has_table(table):
        return set()
    return {column["name"] for column in _inspector().get_columns(table)}


def _indexes(table):
    if not _has_table(table):
        return set()
    return {index["name"] for index in _inspector().get_indexes(table)}


def _add_column_if_missing(table, column):
    if not _has_table(table) or column.name in _columns(table):
        return
    with op.batch_alter_table(table) as batch:
        batch.add_column(column)


def _create_index_if_missing(table, index_name, columns):
    if not _has_table(table) or index_name in _indexes(table):
        return
    op.create_index(index_name, table, columns)


def _quote(name):
    return '"{}"'.format(name.replace('"', '""'))


def _scalar(sql):
    return op.get_bind().execute(sa.text(sql)).scalar()


def _execute(sql, params=None):
    op.get_bind().execute(sa.text(sql), params or {})


def _backfill_from_parent(child_table, fk_column, parent_table):
    if (
        not _has_table(child_table)
        or not _has_table(parent_table)
        or "tenant_id" not in _columns(child_table)
        or "tenant_id" not in _columns(parent_table)
        or fk_column not in _columns(child_table)
    ):
        return
    child = _quote(child_table)
    parent = _quote(parent_table)
    _execute(
        f"""
        UPDATE {child}
        SET tenant_id = (
            SELECT p.tenant_id FROM {parent} p
            WHERE p.id = {child}.{_quote(fk_column)}
        )
        WHERE tenant_id IS NULL
          AND {_quote(fk_column)} IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM {parent} p
              WHERE p.id = {child}.{_quote(fk_column)}
                AND p.tenant_id IS NOT NULL
          )
        """
    )


def _backfill_single_tenant_defaults():
    tenant_count = int(_scalar('SELECT COUNT(*) FROM "tenant"') or 0) if _has_table("tenant") else 0
    if tenant_count != 1:
        return
    tenant_id = _scalar('SELECT "id" FROM "tenant" ORDER BY "id" ASC LIMIT 1')
    for table in TENANT_TABLES:
        if _has_table(table) and "tenant_id" in _columns(table):
            _execute(
                f"UPDATE {_quote(table)} SET tenant_id = :tenant_id WHERE tenant_id IS NULL",
                {"tenant_id": tenant_id},
            )


def _create_unique_indexes():
    for index_name, table, columns, predicate in TENANT_UNIQUE_INDEXES:
        if not _has_table(table) or index_name in _indexes(table):
            continue
        cols_sql = ", ".join(columns)
        if _dialect() == "postgresql":
            op.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" ON {_quote(table)} ({cols_sql}) WHERE {predicate}')
        else:
            plain_columns = [column for column in columns if "(" not in column]
            if len(plain_columns) == len(columns):
                op.create_index(index_name, table, plain_columns, unique=True)


def _enable_postgres_rls():
    if _dialect() != "postgresql":
        return
    for table in TENANT_TABLES:
        if not _has_table(table) or "tenant_id" not in _columns(table):
            continue
        policy_name = f"tenant_isolation_{table}"
        table_sql = _quote(table)
        tenant_expr = (
            "NULLIF(current_setting('app.current_tenant_id', true), '')::integer"
        )
        if table == "login_security_event":
            using_expr = f"(tenant_id IS NULL OR tenant_id = {tenant_expr})"
            check_expr = f"(tenant_id IS NULL OR tenant_id = {tenant_expr})"
        else:
            using_expr = f"(tenant_id = {tenant_expr})"
            check_expr = using_expr
        op.execute(f"ALTER TABLE {table_sql} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table_sql} FORCE ROW LEVEL SECURITY")
        op.execute(f"DROP POLICY IF EXISTS {policy_name} ON {table_sql}")
        op.execute(
            f"CREATE POLICY {policy_name} ON {table_sql} "
            f"FOR ALL USING ({using_expr}) WITH CHECK ({check_expr})"
        )


def upgrade():
    for table in TENANT_TABLES:
        _add_column_if_missing(table, sa.Column("tenant_id", sa.Integer(), nullable=True))
        _create_index_if_missing(table, f"ix_{table}_tenant_id", ["tenant_id"])

    for child_table, fk_column, parent_table in PARENT_BACKFILLS:
        _backfill_from_parent(child_table, fk_column, parent_table)
    _backfill_single_tenant_defaults()
    _create_unique_indexes()
    _enable_postgres_rls()


def downgrade():
    if _dialect() == "postgresql":
        for table in TENANT_TABLES:
            if _has_table(table):
                table_sql = _quote(table)
                op.execute(f"DROP POLICY IF EXISTS tenant_isolation_{table} ON {table_sql}")
                op.execute(f"ALTER TABLE {table_sql} NO FORCE ROW LEVEL SECURITY")
                op.execute(f"ALTER TABLE {table_sql} DISABLE ROW LEVEL SECURITY")
