"""Tenant foundation and tenant-aware users.

Revision ID: 20261009_13
Revises: 20261002_11
Create Date: 2026-10-09
"""

from datetime import date, datetime
import uuid

from alembic import op
import sqlalchemy as sa


revision = "20261009_13"
down_revision = "20261002_11"
branch_labels = None
depends_on = None


TENANT_OWNED_TABLES = (
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
)


def _inspector():
    return sa.inspect(op.get_bind())


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


def _create_index_if_missing(table, index_name, columns, unique=False):
    if not _has_table(table) or index_name in _indexes(table):
        return
    op.create_index(index_name, table, columns, unique=unique)


def _scalar(sql):
    return op.get_bind().execute(sa.text(sql)).scalar()


def upgrade():
    if not _has_table("tenant_settings"):
        op.create_table(
            "tenant_settings",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("settings_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )

    if not _has_table("tenant"):
        op.create_table(
            "tenant",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("tenant_uid", sa.String(40), nullable=False),
            sa.Column("tenant_code", sa.String(80), nullable=False),
            sa.Column("pharmacy_name", sa.String(160), nullable=False),
            sa.Column("owner_name", sa.String(120), nullable=True),
            sa.Column("contact_number", sa.String(30), nullable=True),
            sa.Column("email", sa.String(160), nullable=True),
            sa.Column("business_address", sa.Text(), nullable=True),
            sa.Column("gstin", sa.String(30), nullable=True),
            sa.Column("drug_license", sa.String(80), nullable=True),
            sa.Column("subscription_plan_ref", sa.String(80), nullable=True),
            sa.Column("subscription_start", sa.Date(), nullable=True),
            sa.Column("subscription_expiry", sa.Date(), nullable=True),
            sa.Column("status", sa.String(20), nullable=False, server_default="Trial"),
            sa.Column("settings_id", sa.Integer(), sa.ForeignKey("tenant_settings.id"), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("activated_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )
    _create_index_if_missing("tenant", "ix_tenant_tenant_uid", ["tenant_uid"], unique=True)
    _create_index_if_missing("tenant", "ix_tenant_tenant_code", ["tenant_code"], unique=True)
    _create_index_if_missing("tenant", "ix_tenant_pharmacy_name", ["pharmacy_name"])
    _create_index_if_missing("tenant", "ix_tenant_email", ["email"])
    _create_index_if_missing("tenant", "ix_tenant_subscription_expiry", ["subscription_expiry"])
    _create_index_if_missing("tenant", "ix_tenant_status", ["status"])
    _create_index_if_missing("tenant", "ix_tenant_created_at", ["created_at"])
    _create_index_if_missing("tenant", "ix_tenant_activated_at", ["activated_at"])

    _add_column_if_missing("user", sa.Column("tenant_id", sa.Integer(), nullable=True))
    _create_index_if_missing("user", "ix_user_tenant_id", ["tenant_id"])

    for table_name in TENANT_OWNED_TABLES:
        _add_column_if_missing(table_name, sa.Column("tenant_id", sa.Integer(), nullable=True))
        _create_index_if_missing(table_name, f"ix_{table_name}_tenant_id", ["tenant_id"])

    tenant_count = int(_scalar('SELECT COUNT(*) FROM "tenant"') or 0)
    user_count = int(_scalar('SELECT COUNT(*) FROM "user"') or 0) if _has_table("user") else 0
    if tenant_count == 0 and user_count > 0:
        now = datetime.utcnow()
        conn = op.get_bind()
        conn.execute(
            sa.text(
                'INSERT INTO "tenant_settings" ("settings_json", "created_at", "updated_at") '
                'VALUES (:settings_json, :created_at, :updated_at)'
            ),
            {"settings_json": "{}", "created_at": now, "updated_at": now},
        )
        settings_id = _scalar('SELECT MAX("id") FROM "tenant_settings"')
        tenant_uid = "TEN-" + uuid.uuid4().hex[:24].upper()
        conn.execute(
            sa.text(
                'INSERT INTO "tenant" '
                '("tenant_uid", "tenant_code", "pharmacy_name", "subscription_plan_ref", '
                '"subscription_start", "status", "settings_id", "created_at", "activated_at", "updated_at") '
                'VALUES (:tenant_uid, :tenant_code, :pharmacy_name, :plan, :start, :status, '
                ':settings_id, :created_at, :activated_at, :updated_at)'
            ),
            {
                "tenant_uid": tenant_uid,
                "tenant_code": "default",
                "pharmacy_name": "The Endo Pharmacy",
                "plan": "legacy",
                "start": date.today(),
                "status": "Active",
                "settings_id": settings_id,
                "created_at": now,
                "activated_at": now,
                "updated_at": now,
            },
        )
        tenant_id = _scalar('SELECT MAX("id") FROM "tenant"')
        conn.execute(
            sa.text('UPDATE "user" SET "tenant_id" = :tenant_id WHERE "tenant_id" IS NULL'),
            {"tenant_id": tenant_id},
        )


def downgrade():
    # Conservative: tenant identifiers and ownership columns are future
    # isolation metadata. Do not drop them automatically.
    pass
