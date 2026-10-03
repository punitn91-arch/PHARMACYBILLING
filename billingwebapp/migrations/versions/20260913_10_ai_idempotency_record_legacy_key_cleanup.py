"""Fix ai_idempotency_record's legacy idempotency_key column blocking all new AI mutations.

Revision ID: 20260913_10
Revises: 20260910_09
Create Date: 2026-09-13

Some existing installs still carry `ai_idempotency_record`'s original,
pre-"ai_api_foundation" column set (`idempotency_key` NOT NULL,
`status_code`, integer `resource_id`) from before that table was
redesigned around `idempotency_key_hash`. 20260831_05
("ai_legacy_schema_reconciliation") added the new columns onto those
installs but, matching that migration's explicitly additive/non-destructive
design ("draft columns and rows are never removed"), never touched the old
ones.

That left a real defect on any install with the legacy column: current
code (`begin_idempotency` in `services/ai_appointment_service.py`) only
ever sets `idempotency_key_hash`, never the old `idempotency_key` -- so on
those installs *every* first attempt at *any* AI-mutation idempotency
insert (appointment booking, callback, complaint, waitlist, ...) fails its
NOT NULL constraint. `begin_idempotency`'s generic `except IntegrityError`
handler then reports that as the misleading "REQUEST_IN_PROGRESS" (a
request that never actually started). Confirmed by direct reproduction
against a fresh idempotency key on both `POST /appointments` and the
pre-existing `POST /callbacks` route -- this is not specific to any one
endpoint, and not something appointment-booking code caused.

The live unique constraint on affected installs is also still keyed on the
legacy `idempotency_key` column (`uq_ai_idempotency_client_operation_key`),
not `idempotency_key_hash` as the current model declares -- so even
duplicate-key protection is checking the wrong column (new rows always
leave `idempotency_key` NULL, and SQLite does not treat NULLs as
duplicates, so that constraint currently protects nothing for new writes).

Fix (guarded -- only touches installs that still have the legacy column):
- Relax `idempotency_key` to nullable so new rows (which never populate
  it) succeed. Never dropped -- any existing legacy rows and their data
  are left exactly as they are, matching this migration history's
  established non-destructive convention.
- Rebuild the unique constraint on `(api_client_id, operation,
  idempotency_key_hash)`, matching the current model, so duplicate-key
  detection actually works at the database level for current code.

Installs that already match the current model (a fresh `db.create_all()`,
or any install that never had the legacy column) have no `idempotency_key`
column at all -- this migration is a straight no-op for them. It is also
safe to run more than once anywhere: it detects the already-fixed state
and does nothing.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260913_10"
down_revision = "20260910_09"
branch_labels = None
depends_on = None


_TABLE = "ai_idempotency_record"
_LEGACY_UNIQUE_CONSTRAINT = "uq_ai_idempotency_client_operation_key"
_CORRECT_COLUMNS = ["api_client_id", "operation", "idempotency_key_hash"]


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
    if "idempotency_key" not in columns:
        # Already on the current, clean schema -- nothing to reconcile.
        return

    legacy_key_nullable = bool(columns["idempotency_key"]["nullable"])
    constraints = _unique_constraints(_TABLE)
    legacy_constraint = constraints.get(_LEGACY_UNIQUE_CONSTRAINT)
    constraint_needs_fix = legacy_constraint is not None and list(
        legacy_constraint.get("column_names") or []
    ) != _CORRECT_COLUMNS

    if legacy_key_nullable and not constraint_needs_fix:
        # Already reconciled by a previous run of this exact migration.
        return

    with op.batch_alter_table(_TABLE, recreate="always") as batch:
        if not legacy_key_nullable:
            batch.alter_column(
                "idempotency_key",
                existing_type=sa.String(length=128),
                nullable=True,
            )
        if constraint_needs_fix:
            batch.drop_constraint(_LEGACY_UNIQUE_CONSTRAINT, type_="unique")
            batch.create_unique_constraint(_LEGACY_UNIQUE_CONSTRAINT, _CORRECT_COLUMNS)


def downgrade():
    # Non-destructive by design, matching 20260831_05: relaxing a NOT NULL
    # constraint and repointing a unique constraint at the correct column
    # is not meaningfully reversible without reintroducing the defect this
    # migration exists to fix, and no data is lost by leaving it as-is.
    pass
