"""Keep OTP verification sessions compatible with legacy clinic databases.

Revision ID: 20260908_08
Revises: 20260905_07
Create Date: 2026-09-08

Some early installations already have a non-null ``verified_at`` field on
``patient_verification_session``.  The application always writes that field,
and this migration adds it for installations created by the newer schema.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260908_08"
down_revision = "20260905_07"
branch_labels = None
depends_on = None


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if "patient_verification_session" not in inspector.get_table_names():
        return
    columns = {
        column["name"]
        for column in inspector.get_columns("patient_verification_session")
    }
    if "verified_at" not in columns:
        # Nullable retains compatibility with SQLite's additive ALTER TABLE;
        # the application supplies a timestamp for every new row.
        op.add_column(
            "patient_verification_session",
            sa.Column("verified_at", sa.DateTime(), nullable=True),
        )
    refreshed = sa.inspect(op.get_bind())
    indexes = {
        index["name"]
        for index in refreshed.get_indexes("patient_verification_session")
    }
    if "ix_patient_verification_session_verified_at" not in indexes:
        op.create_index(
            "ix_patient_verification_session_verified_at",
            "patient_verification_session",
            ["verified_at"],
            unique=False,
        )


def downgrade():
    # Do not remove a field that may contain audit-relevant verification data.
    pass
