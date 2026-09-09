"""Keep legacy and finalized clinic timezone columns compatible.

Revision ID: 20260901_06
Revises: 20260831_05

Older installations created ``clinic_profile.timezone`` as NOT NULL before
the finalized integration introduced ``timezone_name``. SQLite rejects new
profiles when an ORM INSERT omits that legacy column. This migration is
strictly additive and synchronizes both values without rebuilding the table.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260901_06"
down_revision = "20260831_05"
branch_labels = None
depends_on = None


def _columns():
    inspector = sa.inspect(op.get_bind())
    if "clinic_profile" not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns("clinic_profile")}


def upgrade():
    columns = _columns()
    if not columns:
        return
    if "timezone" not in columns:
        op.add_column(
            "clinic_profile",
            sa.Column(
                "timezone",
                sa.String(64),
                nullable=False,
                server_default="Asia/Kolkata",
            ),
        )
        columns.add("timezone")
    if "timezone_name" not in columns:
        op.add_column(
            "clinic_profile",
            sa.Column(
                "timezone_name",
                sa.String(64),
                nullable=False,
                server_default="Asia/Kolkata",
            ),
        )
        columns.add("timezone_name")
    if {"timezone", "timezone_name"}.issubset(columns):
        op.execute(
            sa.text(
                "UPDATE clinic_profile "
                "SET timezone_name = COALESCE(NULLIF(timezone_name, ''), NULLIF(timezone, ''), 'Asia/Kolkata'), "
                "timezone = COALESCE(NULLIF(timezone_name, ''), NULLIF(timezone, ''), 'Asia/Kolkata')"
            )
        )


def downgrade():
    # The compatibility column may contain pre-existing production data and
    # can be NOT NULL in legacy SQLite schemas. Never drop it automatically.
    pass
