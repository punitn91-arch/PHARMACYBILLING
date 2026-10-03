"""Fix appointment_slot_lock's legacy start_time column blocking new bookings.

Revision ID: 20260913_11
Revises: 20260913_10
Create Date: 2026-09-13

The exact same defect as 20260913_10 (`ai_idempotency_record`), found while
end-to-end-verifying a real booking against a database that had that first
fix applied: `appointment_slot_lock` still carries its original
`start_time` column (NOT NULL) on some installs, from before it was
renamed to `slot_time` -- 20260831_05 added `slot_time` alongside it
without touching `start_time`, again matching that migration's explicitly
additive/non-destructive design.

Current code (`_acquire_slot_lock` in `services/ai_appointment_service.py`)
only ever sets `slot_time`, never `start_time` -- so on affected installs,
the very first booking attempt for any not-yet-locked (doctor, location,
date, time) combination fails that NOT NULL constraint on insert. The
`except IntegrityError: lock = None` in `_acquire_slot_lock` swallows this
(it exists to tolerate a genuine concurrent-insert race, not this), and
the function then falls through to `query.with_for_update().one()`, which
raises `NoResultFound` -- surfaced to the caller as an opaque 500. This is
not specific to the new no-OTP contact-based booking path added this
task; it affects booking through the pre-existing OTP-verified path
identically, for any slot no earlier caller has already locked.

The live unique constraint on affected installs is also still keyed on
`start_time` (`uq_appointment_slot_lock_scope`), not `slot_time` as the
current model declares.

Fix (guarded -- only touches installs that still have the legacy column),
identical in shape to 20260913_10:
- Relax `start_time` to nullable so new rows succeed. Never dropped.
- Rebuild the unique constraint on `(clinician_id, location_id,
  appointment_date, slot_time)`, matching the current model.

A no-op for installs that already match the current model, and safe to
run more than once anywhere.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260913_11"
down_revision = "20260913_10"
branch_labels = None
depends_on = None


_TABLE = "appointment_slot_lock"
_LEGACY_UNIQUE_CONSTRAINT = "uq_appointment_slot_lock_scope"
_CORRECT_COLUMNS = ["clinician_id", "location_id", "appointment_date", "slot_time"]


def _inspector():
    return sa.inspect(op.get_bind())


def _has_table(name):
    return name in _inspector().get_table_names()


def _columns(table):
    return {column["name"]: column for column in _inspector().get_columns(table)}


def _unique_constraints(table):
    return {uc["name"]: uc for uc in _inspector().get_unique_constraints(table) if uc.get("name")}


def upgrade():
    if not _has_table(_TABLE):
        return
    columns = _columns(_TABLE)
    if "start_time" not in columns:
        return

    legacy_nullable = bool(columns["start_time"]["nullable"])
    constraints = _unique_constraints(_TABLE)
    legacy_constraint = constraints.get(_LEGACY_UNIQUE_CONSTRAINT)
    constraint_needs_fix = legacy_constraint is not None and list(
        legacy_constraint.get("column_names") or []
    ) != _CORRECT_COLUMNS

    if legacy_nullable and not constraint_needs_fix:
        return

    with op.batch_alter_table(_TABLE, recreate="always") as batch:
        if not legacy_nullable:
            batch.alter_column("start_time", existing_type=sa.Time(), nullable=True)
        if constraint_needs_fix:
            batch.drop_constraint(_LEGACY_UNIQUE_CONSTRAINT, type_="unique")
            batch.create_unique_constraint(_LEGACY_UNIQUE_CONSTRAINT, _CORRECT_COLUMNS)


def downgrade():
    # Non-destructive by design -- see 20260913_10's downgrade for the same
    # reasoning.
    pass
