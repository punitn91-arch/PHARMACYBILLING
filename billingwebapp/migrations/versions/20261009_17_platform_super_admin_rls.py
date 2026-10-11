"""Allow only global platform administrators outside a tenant context.

Revision ID: 20261009_17
Revises: 20261009_16
Create Date: 2026-10-09
"""

from alembic import op


revision = "20261009_17"
down_revision = "20261009_16"
branch_labels = None
depends_on = None


POLICIES = (
    "platform_super_admin_select",
    "platform_super_admin_insert",
    "platform_super_admin_update",
)


def upgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return

    tenant_context = (
        "NULLIF(current_setting('app.current_tenant_id', true), '')::integer"
    )
    platform_owner = (
        f"(tenant_id IS NULL AND lower(role) = 'super_admin' "
        f"AND {tenant_context} IS NULL)"
    )

    for policy_name in POLICIES:
        op.execute(f'DROP POLICY IF EXISTS "{policy_name}" ON "user"')

    op.execute(
        f'CREATE POLICY "{POLICIES[0]}" ON "user" FOR SELECT '
        f"USING ({platform_owner})"
    )
    op.execute(
        f'CREATE POLICY "{POLICIES[1]}" ON "user" FOR INSERT '
        f"WITH CHECK ({platform_owner})"
    )
    op.execute(
        f'CREATE POLICY "{POLICIES[2]}" ON "user" FOR UPDATE '
        f"USING ({platform_owner}) WITH CHECK ({platform_owner})"
    )


def downgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return

    for policy_name in POLICIES:
        op.execute(f'DROP POLICY IF EXISTS "{policy_name}" ON "user"')
