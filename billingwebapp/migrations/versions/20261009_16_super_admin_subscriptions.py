"""Super admin subscription history and platform owner support.

Revision ID: 20261009_16
Revises: 20261009_15
Create Date: 2026-10-09
"""

from alembic import op
import sqlalchemy as sa


revision = "20261009_16"
down_revision = "20261009_15"
branch_labels = None
depends_on = None


def _bind():
    return op.get_bind()


def _inspector():
    return sa.inspect(_bind())


def _has_table(table):
    return table in _inspector().get_table_names()


def _indexes(table):
    if not _has_table(table):
        return set()
    return {idx["name"] for idx in _inspector().get_indexes(table)}


def upgrade():
    if not _has_table("subscription_history"):
        op.create_table(
            "subscription_history",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenant.id"), nullable=False),
            sa.Column("plan_ref", sa.String(length=80), nullable=False),
            sa.Column("plan_name", sa.String(length=120), nullable=False),
            sa.Column("billing_cycle", sa.String(length=30), nullable=False, server_default="trial"),
            sa.Column("start_date", sa.Date(), nullable=False),
            sa.Column("expiry_date", sa.Date(), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False, server_default="Active"),
            sa.Column("user_limit", sa.Integer(), nullable=True),
            sa.Column("feature_entitlements_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("amount", sa.Numeric(12, 2), nullable=False, server_default="0"),
            sa.Column("payment_mode", sa.String(length=40), nullable=True),
            sa.Column("payment_reference", sa.String(length=120), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_by", sa.String(length=50), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )
    for index_name, columns in (
        ("ix_subscription_history_tenant_id", ["tenant_id"]),
        ("ix_subscription_history_plan_ref", ["plan_ref"]),
        ("ix_subscription_history_billing_cycle", ["billing_cycle"]),
        ("ix_subscription_history_start_date", ["start_date"]),
        ("ix_subscription_history_expiry_date", ["expiry_date"]),
        ("ix_subscription_history_status", ["status"]),
        ("ix_subscription_history_created_at", ["created_at"]),
    ):
        if _has_table("subscription_history") and index_name not in _indexes("subscription_history"):
            op.create_index(index_name, "subscription_history", columns)


def downgrade():
    if _has_table("subscription_history"):
        op.drop_table("subscription_history")
