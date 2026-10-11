"""Enforce tenant RLS on settings and subscription history.

Revision ID: 20261009_18
Revises: 20261009_17
Create Date: 2026-10-09
"""

from alembic import op


revision = "20261009_18"
down_revision = "20261009_17"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return

    tenant_context = (
        "NULLIF(current_setting('app.current_tenant_id', true), '')::integer"
    )
    platform_admin = (
        "COALESCE(current_setting('app.is_platform_admin', true), '') = 'true'"
    )
    subscription_access = (
        f"(tenant_id = {tenant_context} OR ({platform_admin}))"
    )
    settings_access = (
        "EXISTS ("
        'SELECT 1 FROM "tenant" AS owning_tenant '
        'WHERE owning_tenant.settings_id = "tenant_settings".id '
        f"AND owning_tenant.id = {tenant_context}"
        f") OR ({platform_admin})"
    )

    op.execute('ALTER TABLE "subscription_history" ENABLE ROW LEVEL SECURITY')
    op.execute('ALTER TABLE "subscription_history" FORCE ROW LEVEL SECURITY')
    op.execute('DROP POLICY IF EXISTS tenant_subscription_history ON "subscription_history"')
    op.execute(
        'CREATE POLICY tenant_subscription_history ON "subscription_history" '
        f"FOR ALL USING ({subscription_access}) "
        f"WITH CHECK ({subscription_access})"
    )

    op.execute('ALTER TABLE "tenant_settings" ENABLE ROW LEVEL SECURITY')
    op.execute('ALTER TABLE "tenant_settings" FORCE ROW LEVEL SECURITY')
    op.execute('DROP POLICY IF EXISTS tenant_settings_isolation ON "tenant_settings"')
    op.execute(
        'CREATE POLICY tenant_settings_isolation ON "tenant_settings" '
        f"FOR ALL USING ({settings_access}) "
        f"WITH CHECK ({settings_access})"
    )


def downgrade():
    if op.get_bind().dialect.name.lower() != "postgresql":
        return

    op.execute('DROP POLICY IF EXISTS tenant_settings_isolation ON "tenant_settings"')
    op.execute('ALTER TABLE "tenant_settings" NO FORCE ROW LEVEL SECURITY')
    op.execute('ALTER TABLE "tenant_settings" DISABLE ROW LEVEL SECURITY')
    op.execute('DROP POLICY IF EXISTS tenant_subscription_history ON "subscription_history"')
    op.execute('ALTER TABLE "subscription_history" NO FORCE ROW LEVEL SECURITY')
    op.execute('ALTER TABLE "subscription_history" DISABLE ROW LEVEL SECURITY')
