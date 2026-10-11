"""Allow narrowly scoped PostgreSQL lookup of AI tenant identity.

Revision ID: 20261010_23
Revises: 20261010_22
"""

from alembic import op
import sqlalchemy as sa


revision = "20261010_23"
down_revision = "20261010_22"
branch_labels = None
depends_on = None


def _has_table(table_name):
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return

    if _has_table("ai_api_client"):
        op.execute(
            "DROP POLICY IF EXISTS ai_client_identity_lookup ON ai_api_client"
        )
        op.execute(
            "CREATE POLICY ai_client_identity_lookup ON ai_api_client "
            "FOR SELECT USING (client_id = "
            "NULLIF(current_setting('app.ai_client_lookup_id', true), ''))"
        )

    if _has_table("ai_access_token"):
        op.execute(
            "DROP POLICY IF EXISTS ai_access_token_identity_lookup ON ai_access_token"
        )
        op.execute(
            "CREATE POLICY ai_access_token_identity_lookup ON ai_access_token "
            "FOR SELECT USING (token_hash = "
            "NULLIF(current_setting('app.ai_access_token_hash', true), ''))"
        )


def downgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return
    if _has_table("ai_access_token"):
        op.execute(
            "DROP POLICY IF EXISTS ai_access_token_identity_lookup ON ai_access_token"
        )
    if _has_table("ai_api_client"):
        op.execute(
            "DROP POLICY IF EXISTS ai_client_identity_lookup ON ai_api_client"
        )
