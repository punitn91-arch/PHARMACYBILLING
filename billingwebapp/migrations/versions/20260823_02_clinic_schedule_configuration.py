"""Add configurable clinic locations, clinicians, and schedule rules.

Revision ID: 20260823_02
Revises: 20260823_01
Create Date: 2026-08-23

The tables hold public-safe operating configuration only. They do not convert
the existing FCFS appointment flow into individual time-slot booking.
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260823_02"
down_revision = "20260823_01"
branch_labels = None
depends_on = None


def _has_table(name):
    return name in sa.inspect(op.get_bind()).get_table_names()


def _create_clinic_location():
    op.create_table(
        "clinic_location",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=40), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("public_address", sa.String(length=255), nullable=True),
        sa.Column("public_phone", sa.String(length=30), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.String(length=50), nullable=True),
        sa.Column("updated_by", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_clinic_location_code", "clinic_location", ["code"], unique=True)
    op.create_index("ix_clinic_location_display_name", "clinic_location", ["display_name"], unique=False)
    op.create_index("ix_clinic_location_is_active", "clinic_location", ["is_active"], unique=False)
    op.create_index("ix_clinic_location_is_default", "clinic_location", ["is_default"], unique=False)
    op.create_index("ix_clinic_location_created_at", "clinic_location", ["created_at"], unique=False)


def _create_clinician():
    op.create_table(
        "clinician",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=40), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("public_title", sa.String(length=80), nullable=True),
        sa.Column("specialty", sa.String(length=120), nullable=True),
        sa.Column("default_location_id", sa.Integer(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.String(length=50), nullable=True),
        sa.Column("updated_by", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["default_location_id"],
            ["clinic_location.id"],
            name="fk_clinician_default_location_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_clinician_code", "clinician", ["code"], unique=True)
    op.create_index("ix_clinician_display_name", "clinician", ["display_name"], unique=False)
    op.create_index("ix_clinician_specialty", "clinician", ["specialty"], unique=False)
    op.create_index("ix_clinician_default_location_id", "clinician", ["default_location_id"], unique=False)
    op.create_index("ix_clinician_is_active", "clinician", ["is_active"], unique=False)
    op.create_index("ix_clinician_created_at", "clinician", ["created_at"], unique=False)


def _create_clinic_schedule_rule():
    op.create_table(
        "clinic_schedule_rule",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.Integer(), nullable=False),
        sa.Column("clinician_id", sa.Integer(), nullable=True),
        sa.Column("weekday", sa.Integer(), nullable=False),
        sa.Column("booking_enabled", sa.Boolean(), nullable=False),
        sa.Column("arrival_window_start", sa.String(length=5), nullable=False),
        sa.Column("arrival_window_end", sa.String(length=5), nullable=False),
        sa.Column("normal_daily_limit", sa.Integer(), nullable=True),
        sa.Column("priority_daily_limit", sa.Integer(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("public_note", sa.String(length=240), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.String(length=50), nullable=True),
        sa.Column("updated_by", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["clinician_id"],
            ["clinician.id"],
            name="fk_clinic_schedule_rule_clinician_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["clinic_location.id"],
            name="fk_clinic_schedule_rule_location_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for index_name, column in (
        ("ix_clinic_schedule_rule_location_id", "location_id"),
        ("ix_clinic_schedule_rule_clinician_id", "clinician_id"),
        ("ix_clinic_schedule_rule_weekday", "weekday"),
        ("ix_clinic_schedule_rule_booking_enabled", "booking_enabled"),
        ("ix_clinic_schedule_rule_effective_from", "effective_from"),
        ("ix_clinic_schedule_rule_effective_to", "effective_to"),
        ("ix_clinic_schedule_rule_is_active", "is_active"),
        ("ix_clinic_schedule_rule_created_at", "created_at"),
    ):
        op.create_index(index_name, "clinic_schedule_rule", [column], unique=False)


def _create_clinic_schedule_exception():
    op.create_table(
        "clinic_schedule_exception",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.Integer(), nullable=False),
        sa.Column("clinician_id", sa.Integer(), nullable=True),
        sa.Column("schedule_date", sa.Date(), nullable=False),
        sa.Column("exception_type", sa.String(length=20), nullable=False),
        sa.Column("booking_enabled", sa.Boolean(), nullable=True),
        sa.Column("arrival_window_start", sa.String(length=5), nullable=True),
        sa.Column("arrival_window_end", sa.String(length=5), nullable=True),
        sa.Column("normal_daily_limit", sa.Integer(), nullable=True),
        sa.Column("priority_daily_limit", sa.Integer(), nullable=True),
        sa.Column("public_note", sa.String(length=240), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.String(length=50), nullable=True),
        sa.Column("updated_by", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["clinician_id"],
            ["clinician.id"],
            name="fk_clinic_schedule_exception_clinician_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["clinic_location.id"],
            name="fk_clinic_schedule_exception_location_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for index_name, column in (
        ("ix_clinic_schedule_exception_location_id", "location_id"),
        ("ix_clinic_schedule_exception_clinician_id", "clinician_id"),
        ("ix_clinic_schedule_exception_schedule_date", "schedule_date"),
        ("ix_clinic_schedule_exception_exception_type", "exception_type"),
        ("ix_clinic_schedule_exception_booking_enabled", "booking_enabled"),
        ("ix_clinic_schedule_exception_is_active", "is_active"),
        ("ix_clinic_schedule_exception_created_at", "created_at"),
    ):
        op.create_index(index_name, "clinic_schedule_exception", [column], unique=False)


def upgrade():
    if not _has_table("clinic_location"):
        _create_clinic_location()
    if not _has_table("clinician"):
        _create_clinician()
    if not _has_table("clinic_schedule_rule"):
        _create_clinic_schedule_rule()
    if not _has_table("clinic_schedule_exception"):
        _create_clinic_schedule_exception()


def downgrade():
    if _has_table("clinic_schedule_exception"):
        op.drop_table("clinic_schedule_exception")
    if _has_table("clinic_schedule_rule"):
        op.drop_table("clinic_schedule_rule")
    if _has_table("clinician"):
        op.drop_table("clinician")
    if _has_table("clinic_location"):
        op.drop_table("clinic_location")
