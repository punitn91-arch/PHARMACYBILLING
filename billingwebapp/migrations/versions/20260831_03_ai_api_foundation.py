"""Add secure AI integration API identities, tokens, audit and idempotency.

Revision ID: 20260831_03
Revises: 20260823_02
Create Date: 2026-08-31
"""

from alembic import op
import sqlalchemy as sa


revision = "20260831_03"
down_revision = "20260823_02"
branch_labels = None
depends_on = None


def _has_table(name):
    return name in sa.inspect(op.get_bind()).get_table_names()


def _create_ai_api_client():
    op.create_table(
        "ai_api_client",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("client_id", sa.String(length=80), nullable=False),
        sa.Column("secret_hash", sa.String(length=512), nullable=False),
        sa.Column("allowed_scopes_json", sa.Text(), nullable=False),
        sa.Column("allowed_ips_json", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("secret_version", sa.Integer(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(length=50), nullable=True),
        sa.Column("updated_by", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ai_api_client_client_id", "ai_api_client", ["client_id"], unique=True)
    op.create_index("ix_ai_api_client_is_active", "ai_api_client", ["is_active"], unique=False)
    op.create_index("ix_ai_api_client_last_used_at", "ai_api_client", ["last_used_at"], unique=False)
    op.create_index("ix_ai_api_client_created_at", "ai_api_client", ["created_at"], unique=False)


def _create_ai_access_token():
    op.create_table(
        "ai_access_token",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("api_client_id", sa.Integer(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("scopes_json", sa.Text(), nullable=False),
        sa.Column("secret_version", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["api_client_id"],
            ["ai_api_client.id"],
            name="fk_ai_access_token_api_client_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ai_access_token_api_client_id", "ai_access_token", ["api_client_id"], unique=False)
    op.create_index("ix_ai_access_token_token_hash", "ai_access_token", ["token_hash"], unique=True)
    op.create_index("ix_ai_access_token_expires_at", "ai_access_token", ["expires_at"], unique=False)
    op.create_index("ix_ai_access_token_revoked_at", "ai_access_token", ["revoked_at"], unique=False)
    op.create_index("ix_ai_access_token_last_used_at", "ai_access_token", ["last_used_at"], unique=False)
    op.create_index("ix_ai_access_token_created_at", "ai_access_token", ["created_at"], unique=False)


def _create_ai_api_request_audit():
    op.create_table(
        "ai_api_request_audit",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("api_client_id", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.String(length=80), nullable=False),
        sa.Column("call_id", sa.String(length=128), nullable=True),
        sa.Column("session_id", sa.String(length=128), nullable=True),
        sa.Column("method", sa.String(length=10), nullable=False),
        sa.Column("endpoint", sa.String(length=255), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=20), nullable=False),
        sa.Column("error_code", sa.String(length=80), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("client_ip_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["api_client_id"],
            ["ai_api_client.id"],
            name="fk_ai_api_request_audit_api_client_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for index_name, column in (
        ("ix_ai_api_request_audit_api_client_id", "api_client_id"),
        ("ix_ai_api_request_audit_request_id", "request_id"),
        ("ix_ai_api_request_audit_call_id", "call_id"),
        ("ix_ai_api_request_audit_session_id", "session_id"),
        ("ix_ai_api_request_audit_endpoint", "endpoint"),
        ("ix_ai_api_request_audit_status_code", "status_code"),
        ("ix_ai_api_request_audit_outcome", "outcome"),
        ("ix_ai_api_request_audit_error_code", "error_code"),
        ("ix_ai_api_request_audit_client_ip_fingerprint", "client_ip_fingerprint"),
        ("ix_ai_api_request_audit_created_at", "created_at"),
    ):
        op.create_index(index_name, "ai_api_request_audit", [column], unique=False)


def _create_ai_idempotency_record():
    op.create_table(
        "ai_idempotency_record",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("api_client_id", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=80), nullable=False),
        sa.Column("idempotency_key_hash", sa.String(length=64), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_json", sa.Text(), nullable=True),
        sa.Column("resource_type", sa.String(length=50), nullable=True),
        sa.Column("resource_id", sa.String(length=80), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["api_client_id"],
            ["ai_api_client.id"],
            name="fk_ai_idempotency_record_api_client_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "api_client_id",
            "operation",
            "idempotency_key_hash",
            name="uq_ai_idempotency_client_operation_key",
        ),
    )
    for index_name, column in (
        ("ix_ai_idempotency_record_api_client_id", "api_client_id"),
        ("ix_ai_idempotency_record_operation", "operation"),
        ("ix_ai_idempotency_record_idempotency_key_hash", "idempotency_key_hash"),
        ("ix_ai_idempotency_record_state", "state"),
        ("ix_ai_idempotency_record_resource_id", "resource_id"),
        ("ix_ai_idempotency_record_expires_at", "expires_at"),
        ("ix_ai_idempotency_record_created_at", "created_at"),
    ):
        op.create_index(index_name, "ai_idempotency_record", [column], unique=False)


def upgrade():
    if not _has_table("ai_api_client"):
        _create_ai_api_client()
    if not _has_table("ai_access_token"):
        _create_ai_access_token()
    if not _has_table("ai_api_request_audit"):
        _create_ai_api_request_audit()
    if not _has_table("ai_idempotency_record"):
        _create_ai_idempotency_record()


def downgrade():
    for table_name in (
        "ai_idempotency_record",
        "ai_api_request_audit",
        "ai_access_token",
        "ai_api_client",
    ):
        if _has_table(table_name):
            op.drop_table(table_name)

