"""Add tenant ownership foreign keys to tenant-scoped tables.

Revision ID: 20261009_19
Revises: 20261009_18
Create Date: 2026-10-09
"""

from alembic import op
import sqlalchemy as sa


revision = "20261009_19"
down_revision = "20261009_18"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def upgrade():
    inspector = _inspector()
    if "tenant" not in inspector.get_table_names():
        raise RuntimeError("Cannot add tenant ownership foreign keys without the tenant table")

    for table_name in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns(table_name)}
        if table_name == "tenant" or "tenant_id" not in columns:
            continue

        has_tenant_foreign_key = any(
            "tenant_id" in (foreign_key.get("constrained_columns") or ())
            and foreign_key.get("referred_table") == "tenant"
            and (foreign_key.get("referred_columns") or ()) == ["id"]
            for foreign_key in inspector.get_foreign_keys(table_name)
        )
        if has_tenant_foreign_key:
            continue

        with op.batch_alter_table(table_name) as batch:
            batch.create_foreign_key(
                f"fk_{table_name}_tenant_id_tenant",
                "tenant",
                ["tenant_id"],
                ["id"],
            )


def downgrade():
    inspector = _inspector()
    for table_name in inspector.get_table_names():
        expected_name = f"fk_{table_name}_tenant_id_tenant"
        for foreign_key in inspector.get_foreign_keys(table_name):
            if (
                "tenant_id" in (foreign_key.get("constrained_columns") or ())
                and foreign_key.get("referred_table") == "tenant"
                and (foreign_key.get("referred_columns") or ()) == ["id"]
                and foreign_key.get("name") == expected_name
            ):
                with op.batch_alter_table(table_name) as batch:
                    batch.drop_constraint(expected_name, type_="foreignkey")
