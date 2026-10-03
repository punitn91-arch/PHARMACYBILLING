"""Add return exchange billing adjustment fields.

Revision ID: 20261002_10
Revises: 20260913_12
Create Date: 2026-10-02
"""

from alembic import op
import sqlalchemy as sa


revision = "20261002_10"
down_revision = "20260913_12"
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
    if column.name in _columns(table):
        return
    with op.batch_alter_table(table) as batch:
        batch.add_column(column)


def upgrade():
    _add_column_if_missing("invoice", sa.Column("return_credit_used", sa.Float(), server_default="0"))
    _add_column_if_missing("invoice", sa.Column("final_payable", sa.Float(), server_default="0"))
    _add_column_if_missing("invoice", sa.Column("refund_amount", sa.Float(), server_default="0"))

    _add_column_if_missing("return_bill", sa.Column("adjusted_invoice_id", sa.Integer(), nullable=True))
    _add_column_if_missing("return_bill", sa.Column("adjusted_amount", sa.Float(), server_default="0"))
    _add_column_if_missing("return_bill", sa.Column("refund_amount", sa.Float(), server_default="0"))
    _add_column_if_missing("return_bill", sa.Column("cash_refund_amount", sa.Numeric(10, 2), server_default="0"))
    _add_column_if_missing("return_bill", sa.Column("online_refund_amount", sa.Numeric(10, 2), server_default="0"))
    _add_column_if_missing("return_bill", sa.Column("is_split_refund", sa.Boolean(), server_default=sa.false()))


def downgrade():
    # Keep this downgrade conservative. These fields are audit/payment
    # snapshots; dropping them would remove exchange accounting history.
    pass
