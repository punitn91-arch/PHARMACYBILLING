"""Extend clinician, lab test, and schedule-rule tables for the AI upgrade.

Revision ID: 20260905_07
Revises: 20260901_06

Strictly additive. Adds admin-managed public doctor detail (sub-specialties,
languages, conditions treated, services offered), lab test search aliases and
fasting/turnaround fields, and a schedule-rule flag that lets one shared
availability engine serve either an ARRIVAL_WINDOW capacity pool or
individually bookable FIXED_SLOT times. No existing column, table, or row is
dropped or rewritten destructively.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260905_07"
down_revision = "20260901_06"
branch_labels = None
depends_on = None


def _columns(table_name):
    inspector = sa.inspect(op.get_bind())
    if table_name not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade():
    clinician_columns = _columns("clinician")
    if clinician_columns:
        for column_name in (
            "sub_specialties_json",
            "languages_json",
            "conditions_treated_json",
            "services_offered_json",
        ):
            if column_name not in clinician_columns:
                op.add_column(
                    "clinician",
                    sa.Column(column_name, sa.Text(), nullable=False, server_default="[]"),
                )

    lab_test_columns = _columns("lab_test")
    if lab_test_columns:
        if "aliases_json" not in lab_test_columns:
            op.add_column(
                "lab_test",
                sa.Column("aliases_json", sa.Text(), nullable=False, server_default="[]"),
            )
        if "fasting_required" not in lab_test_columns:
            op.add_column(
                "lab_test",
                sa.Column(
                    "fasting_required", sa.Boolean(), nullable=False, server_default=sa.false()
                ),
            )
        if "turnaround_text" not in lab_test_columns:
            op.add_column("lab_test", sa.Column("turnaround_text", sa.String(120), nullable=True))

    rule_columns = _columns("clinic_schedule_rule")
    if rule_columns and "individual_time_slots" not in rule_columns:
        op.add_column(
            "clinic_schedule_rule",
            sa.Column(
                "individual_time_slots", sa.Boolean(), nullable=False, server_default=sa.true()
            ),
        )
        # A rule with no clinician is the clinic-wide open/closed gate, not an
        # individual doctor's bookable calendar. Mark existing clinic-wide
        # rows as an FCFS arrival-window pool, matching this table's own
        # documented contract. Per-doctor rows keep today's FIXED_SLOT
        # behaviour (the server_default above), so no live AI/voice booking
        # answer changes.
        op.execute(
            sa.text(
                "UPDATE clinic_schedule_rule SET individual_time_slots = FALSE "
                "WHERE clinician_id IS NULL"
            )
        )


def downgrade():
    # Additive columns only; existing installations may already have data in
    # them. Never drop columns automatically.
    pass
