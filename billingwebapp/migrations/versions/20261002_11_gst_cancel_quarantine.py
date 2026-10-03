"""Invoice cancellation, per-medicine GST/schedule, return disposition.

Revision ID: 20261002_11
Revises: 20261002_10
Create Date: 2026-10-02
"""

from alembic import op
import sqlalchemy as sa


revision = "20261002_11"
down_revision = "20261002_10"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _has_table(name):
    return name in _inspector().get_table_names()


def _columns(table):
    if not _has_table(table):
        return set()
    return {column["name"] for column in _inspector().get_columns(table)}


def _add_column_if_missing(table, column):
    if not _has_table(table) or column.name in _columns(table):
        return
    with op.batch_alter_table(table) as batch:
        batch.add_column(column)


def upgrade():
    _add_column_if_missing("invoice", sa.Column("is_cancelled", sa.Boolean(), server_default=sa.false()))
    _add_column_if_missing("invoice", sa.Column("cancelled_at", sa.DateTime(), nullable=True))
    _add_column_if_missing("invoice", sa.Column("cancelled_by", sa.String(50), nullable=True))
    _add_column_if_missing("invoice", sa.Column("cancel_reason", sa.String(255), nullable=True))

    _add_column_if_missing("invoice_item", sa.Column("gst_percent", sa.Float(), nullable=True))
    _add_column_if_missing("invoice_item", sa.Column("taxable_amount", sa.Float(), nullable=True))
    _add_column_if_missing("invoice_item", sa.Column("gst_amount", sa.Float(), nullable=True))

    _add_column_if_missing("medicine", sa.Column("gst_percent", sa.Float(), server_default="5"))
    _add_column_if_missing("medicine", sa.Column("schedule_type", sa.String(10), server_default=""))

    _add_column_if_missing("return_item", sa.Column("disposition", sa.String(20), server_default="RESTOCK"))

    if not _has_table("return_lot_allocation"):
        op.create_table(
            "return_lot_allocation",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("return_item_id", sa.Integer(), nullable=False, index=True),
            sa.Column("purchase_item_id", sa.Integer(), nullable=False, index=True),
            sa.Column("qty", sa.Integer(), server_default="0"),
            sa.Column("cost_rate", sa.Float(), server_default="0"),
            sa.Column("created_at", sa.DateTime(), nullable=True),
        )

    if not _has_table("quarantine_stock"):
        op.create_table(
            "quarantine_stock",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("medicine_id", sa.Integer(), index=True),
            sa.Column("medicine_name", sa.String(150), index=True),
            sa.Column("batch", sa.String(50), index=True),
            sa.Column("expiry", sa.String(10)),
            sa.Column("qty", sa.Integer(), server_default="0"),
            sa.Column("reason", sa.String(20)),
            sa.Column("note", sa.String(255)),
            sa.Column("status", sa.String(20), server_default="PENDING", index=True),
            sa.Column("return_id", sa.Integer(), index=True),
            sa.Column("return_item_id", sa.Integer(), index=True),
            sa.Column("invoice_item_id", sa.Integer()),
            sa.Column("cost_rate", sa.Float(), server_default="0"),
            sa.Column("created_by", sa.String(50)),
            sa.Column("created_at", sa.DateTime(), index=True),
            sa.Column("resolved_by", sa.String(50)),
            sa.Column("resolved_at", sa.DateTime()),
        )


def downgrade():
    # Conservative: cancellation flags, GST snapshots and quarantine records are
    # accounting history and must not be dropped automatically.
    pass
