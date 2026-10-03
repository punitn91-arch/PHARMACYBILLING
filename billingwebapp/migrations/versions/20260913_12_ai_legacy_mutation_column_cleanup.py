"""Relax legacy AI mutation columns that current workflows no longer write.

Revision ID: 20260913_12
Revises: 20260913_11
Create Date: 2026-09-13

Some upgraded SQLite installs still contain draft-era mutation columns next
to the finalized AI schema. The current application writes the finalized
columns (callback_ref, complaint_ref, details/report_id, etc.), while the
legacy columns remain data-bearing compatibility fields. Keep those columns,
but relax only the legacy NOT NULL constraints that can reject current AI
production writes.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260913_12"
down_revision = "20260913_11"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _has_table(name):
    return name in _inspector().get_table_names()


def _columns(table):
    return {column["name"]: column for column in _inspector().get_columns(table)}


def _relax_columns(table, definitions):
    if not _has_table(table):
        return
    columns = _columns(table)
    to_relax = [
        (name, existing_type)
        for name, existing_type in definitions
        if name in columns and not bool(columns[name]["nullable"])
    ]
    if not to_relax:
        return
    with op.batch_alter_table(table, recreate="always") as batch:
        for name, existing_type in to_relax:
            batch.alter_column(name, existing_type=existing_type, nullable=True)


def upgrade():
    _relax_columns(
        "callback_request",
        (
            ("reference_no", sa.String(length=40)),
        ),
    )
    _relax_columns(
        "complaint",
        (
            ("reference_no", sa.String(length=40)),
            ("description", sa.String(length=2000)),
        ),
    )
    _relax_columns(
        "secure_document_token",
        (
            ("document_type", sa.String(length=40)),
            ("resource_id", sa.Integer()),
            ("one_time_use", sa.Boolean()),
            ("access_count", sa.Integer()),
        ),
    )


def downgrade():
    # Non-destructive compatibility migration. Reinstating these NOT NULL
    # constraints would re-break current AI writes and could fail if newer
    # rows legitimately leave legacy fields empty.
    pass
