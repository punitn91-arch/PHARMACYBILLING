"""Snapshot tenant business identity fields on issued invoices.

Revision ID: 20261009_20
Revises: 20261009_19
Create Date: 2026-10-09
"""

from alembic import op
import sqlalchemy as sa


revision = "20261009_20"
down_revision = "20261009_19"
branch_labels = None
depends_on = None


SNAPSHOT_COLUMNS = (
    ("print_pharmacy_name", sa.String(length=160)),
    ("print_legal_business_name", sa.String(length=160)),
    ("print_alternate_mobile", sa.String(length=30)),
    ("print_invoice_header", sa.Text()),
    ("print_invoice_footer", sa.Text()),
    ("print_invoice_terms", sa.Text()),
    ("print_authorized_signatory", sa.String(length=120)),
)


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("invoice")}
    for name, column_type in SNAPSHOT_COLUMNS:
        if name not in columns:
            op.add_column("invoice", sa.Column(name, column_type, nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("invoice")}
    for name, _column_type in reversed(SNAPSHOT_COLUMNS):
        if name in columns:
            op.drop_column("invoice", name)
