"""Add the minimal AI call-receptionist persistence foundation.

Revision ID: 20260823_01
Revises:
Create Date: 2026-08-23

This is a guarded bridge migration. The existing application still has a
legacy `db.create_all()` start-up path, so a fresh import may already have
created these tables before Alembic runs. In that case the migration preserves
the tables and upgrades the one pre-release event-fingerprint column safely.
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260823_01"
down_revision = None
branch_labels = None
depends_on = None


def _has_table(name):
    return name in sa.inspect(op.get_bind()).get_table_names()


def _table_columns(name):
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(name)}


def _index_names(name):
    return {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(name)}


def _has_voice_call_event_foreign_key():
    for foreign_key in sa.inspect(op.get_bind()).get_foreign_keys("voice_call_event"):
        if (
            foreign_key.get("referred_table") == "voice_call"
            and foreign_key.get("constrained_columns") == ["voice_call_id"]
        ):
            return True
    return False


def _create_voice_call_table():
    op.create_table(
        "voice_call",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_call_id", sa.String(length=128), nullable=False),
        sa.Column("caller_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("caller_last4", sa.String(length=4), nullable=True),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("language", sa.String(length=12), nullable=True),
        sa.Column("current_intent", sa.String(length=80), nullable=True),
        sa.Column("current_stage", sa.String(length=80), nullable=True),
        sa.Column("context_json", sa.Text(), nullable=True),
        sa.Column("patient_id", sa.Integer(), nullable=True),
        sa.Column("public_booking_id", sa.Integer(), nullable=True),
        sa.Column("verified_at", sa.DateTime(), nullable=True),
        sa.Column("transfer_status", sa.String(length=32), nullable=True),
        sa.Column("outcome", sa.String(length=48), nullable=True),
        sa.Column("error_code", sa.String(length=80), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.Column("last_event_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "provider_call_id", name="uq_voice_call_provider_call_id"),
    )
    for index_name, columns in (
        ("ix_voice_call_provider", ("provider",)),
        ("ix_voice_call_provider_call_id", ("provider_call_id",)),
        ("ix_voice_call_caller_fingerprint", ("caller_fingerprint",)),
        ("ix_voice_call_direction", ("direction",)),
        ("ix_voice_call_status", ("status",)),
        ("ix_voice_call_language", ("language",)),
        ("ix_voice_call_current_intent", ("current_intent",)),
        ("ix_voice_call_current_stage", ("current_stage",)),
        ("ix_voice_call_patient_id", ("patient_id",)),
        ("ix_voice_call_public_booking_id", ("public_booking_id",)),
        ("ix_voice_call_verified_at", ("verified_at",)),
        ("ix_voice_call_transfer_status", ("transfer_status",)),
        ("ix_voice_call_outcome", ("outcome",)),
        ("ix_voice_call_error_code", ("error_code",)),
        ("ix_voice_call_started_at", ("started_at",)),
        ("ix_voice_call_ended_at", ("ended_at",)),
        ("ix_voice_call_last_event_at", ("last_event_at",)),
        ("ix_voice_call_created_at", ("created_at",)),
    ):
        op.create_index(index_name, "voice_call", list(columns), unique=False)


def _create_voice_call_event_table():
    op.create_table(
        "voice_call_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("voice_call_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_event_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(), nullable=False),
        sa.Column("event_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=True),
        sa.Column("processing_status", sa.String(length=32), nullable=False),
        sa.Column("error_code", sa.String(length=80), nullable=True),
        sa.Column("received_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["voice_call_id"],
            ["voice_call.id"],
            name="fk_voice_call_event_voice_call_id",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "provider",
            "provider_event_id",
            name="uq_voice_call_event_provider_event_id",
        ),
    )
    for index_name, columns in (
        ("ix_voice_call_event_voice_call_id", ("voice_call_id",)),
        ("ix_voice_call_event_provider", ("provider",)),
        ("ix_voice_call_event_provider_event_id", ("provider_event_id",)),
        ("ix_voice_call_event_event_type", ("event_type",)),
        ("ix_voice_call_event_occurred_at", ("occurred_at",)),
        ("ix_voice_call_event_event_fingerprint", ("event_fingerprint",)),
        ("ix_voice_call_event_processing_status", ("processing_status",)),
        ("ix_voice_call_event_error_code", ("error_code",)),
        ("ix_voice_call_event_received_at", ("received_at",)),
        ("ix_voice_call_event_processed_at", ("processed_at",)),
        ("ix_voice_call_event_created_at", ("created_at",)),
    ):
        op.create_index(index_name, "voice_call_event", list(columns), unique=False)


def _upgrade_existing_voice_call_event_table():
    """Upgrade the short-lived pre-release table without dropping event rows.

    Earlier local builds stored a plain body SHA in ``payload_sha256``. Empty
    pre-release tables can be renamed safely. If such a table has rows, stop
    before retaining a correlatable raw-body hash under a safer-looking name;
    an operator must first make a reviewed retention decision. A hand-created
    table missing both columns also fails closed when it has rows.
    """

    columns = _table_columns("voice_call_event")
    needs_rename = "event_fingerprint" not in columns and "payload_sha256" in columns
    needs_new_column = "event_fingerprint" not in columns and "payload_sha256" not in columns
    needs_foreign_key = not _has_voice_call_event_foreign_key()

    if needs_rename or needs_new_column:
        row_count = op.get_bind().execute(
            sa.text("SELECT COUNT(*) FROM voice_call_event")
        ).scalar_one()
        if row_count:
            raise RuntimeError(
                "voice_call_event contains pre-release event metadata; make "
                "a reviewed retention decision before upgrading."
            )

    if needs_foreign_key:
        orphan_count = op.get_bind().execute(
            sa.text(
                "SELECT COUNT(*) FROM voice_call_event AS event "
                "LEFT JOIN voice_call AS call ON call.id = event.voice_call_id "
                "WHERE call.id IS NULL"
            )
        ).scalar_one()
        if orphan_count:
            raise RuntimeError(
                "voice_call_event contains orphaned events; repair them "
                "before the call-event foreign key is added."
            )

    if needs_rename or needs_new_column or needs_foreign_key:
        # batch mode works on SQLite and uses native ALTER operations where
        # supported by the production database.
        with op.batch_alter_table("voice_call_event") as batch:
            if needs_rename:
                batch.alter_column(
                    "payload_sha256",
                    new_column_name="event_fingerprint",
                    existing_type=sa.String(length=64),
                    existing_nullable=False,
                )
            elif needs_new_column:
                batch.add_column(
                    sa.Column("event_fingerprint", sa.String(length=64), nullable=False)
                )
            if needs_foreign_key:
                batch.create_foreign_key(
                    "fk_voice_call_event_voice_call_id",
                    "voice_call",
                    ["voice_call_id"],
                    ["id"],
                    ondelete="RESTRICT",
                )

    # The old auto-generated index name can survive a SQLite column rename.
    # Replace it so the physical index matches the ORM model exactly.
    indexes = _index_names("voice_call_event")
    if "ix_voice_call_event_payload_sha256" in indexes:
        op.drop_index("ix_voice_call_event_payload_sha256", table_name="voice_call_event")
        indexes.remove("ix_voice_call_event_payload_sha256")
    if "ix_voice_call_event_event_fingerprint" not in indexes:
        op.create_index(
            "ix_voice_call_event_event_fingerprint",
            "voice_call_event",
            ["event_fingerprint"],
            unique=False,
        )


def upgrade():
    if not _has_table("voice_call"):
        _create_voice_call_table()
    if not _has_table("voice_call_event"):
        _create_voice_call_event_table()
    else:
        _upgrade_existing_voice_call_event_table()


def downgrade():
    # Event rows refer to a call logically, so remove the child inbox first.
    if _has_table("voice_call_event"):
        op.drop_table("voice_call_event")
    if _has_table("voice_call"):
        op.drop_table("voice_call")
