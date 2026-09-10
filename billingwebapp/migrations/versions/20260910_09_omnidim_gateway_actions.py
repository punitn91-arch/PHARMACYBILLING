"""Add replay protection for the OmniDimension gateway.

Revision ID: 20260910_09
Revises: 20260908_08
Create Date: 2026-09-10

The gateway stores only keyed hashes plus the minimal successful response.
Raw caller details, complaint text and transcripts are intentionally not
duplicated into this transport-level table.
"""

from alembic import op
import sqlalchemy as sa


revision = "20260910_09"
down_revision = "20260908_08"
branch_labels = None
depends_on = None


def _has_table(name):
    return name in sa.inspect(op.get_bind()).get_table_names()


def _indexes(table_name):
    return {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table_name)}


def upgrade():
    if not _has_table("omnidim_gateway_action"):
        op.create_table(
            "omnidim_gateway_action",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("operation", sa.String(80), nullable=False),
            sa.Column("idempotency_key_hash", sa.String(64), nullable=False),
            sa.Column("request_fingerprint", sa.String(64), nullable=False),
            sa.Column("state", sa.String(20), nullable=False, server_default="IN_PROGRESS"),
            sa.Column("response_json", sa.Text()),
            sa.Column("response_status", sa.Integer()),
            sa.Column("resource_type", sa.String(50)),
            sa.Column("resource_id", sa.String(80)),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("completed_at", sa.DateTime()),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint(
                "operation",
                "idempotency_key_hash",
                name="uq_omnidim_gateway_action_key",
            ),
        )

    indexes = _indexes("omnidim_gateway_action")
    for name, columns in (
        ("ix_omnidim_gateway_action_operation", ["operation"]),
        ("ix_omnidim_gateway_action_idempotency_key_hash", ["idempotency_key_hash"]),
        ("ix_omnidim_gateway_action_state", ["state"]),
        ("ix_omnidim_gateway_action_resource_type", ["resource_type"]),
        ("ix_omnidim_gateway_action_resource_id", ["resource_id"]),
        ("ix_omnidim_gateway_action_created_at", ["created_at"]),
        ("ix_omnidim_gateway_action_completed_at", ["completed_at"]),
    ):
        if name not in indexes:
            op.create_index(name, "omnidim_gateway_action", columns, unique=False)


def downgrade():
    # Gateway action records are security/audit data.  Never remove them from
    # a production clinic database automatically.
    pass
