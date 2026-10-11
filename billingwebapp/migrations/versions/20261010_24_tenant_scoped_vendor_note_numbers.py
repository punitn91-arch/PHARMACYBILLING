"""Scope vendor-note numbering uniqueness to each tenant.

Revision ID: 20261010_24
Revises: 20261010_23
"""

from alembic import op
import sqlalchemy as sa


revision = "20261010_24"
down_revision = "20261010_23"
branch_labels = None
depends_on = None


def _sqlite_has_global_note_unique(bind):
    for _sequence, index_name, is_unique, origin, _partial in bind.exec_driver_sql(
        'PRAGMA index_list("vendor_notes")'
    ):
        if not is_unique or origin != "u":
            continue
        quoted_index = index_name.replace('"', '""')
        columns = [
            row[2]
            for row in bind.exec_driver_sql(
                f'PRAGMA index_info("{quoted_index}")'
            )
        ]
        if columns == ["note_no"]:
            return True
    return False


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "vendor_notes" not in inspector.get_table_names():
        return

    duplicate = bind.execute(
        sa.text(
            "SELECT tenant_id, note_no FROM vendor_notes "
            "WHERE tenant_id IS NOT NULL GROUP BY tenant_id, note_no "
            "HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicate:
        raise RuntimeError(
            "Cannot scope vendor-note numbers: duplicate numbers already exist within a tenant."
        )

    if any(
        tuple(constraint.get("column_names") or ()) == ("tenant_id", "note_no")
        for constraint in inspector.get_unique_constraints("vendor_notes")
    ):
        return

    global_constraints = [
        constraint
        for constraint in inspector.get_unique_constraints("vendor_notes")
        if tuple(constraint.get("column_names") or ()) == ("note_no",)
    ]
    if bind.dialect.name.lower() == "sqlite":
        if _sqlite_has_global_note_unique(bind):
            violations_before = bind.exec_driver_sql("PRAGMA foreign_key_check").all()
            bind.exec_driver_sql("PRAGMA foreign_keys=OFF")
            if bind.exec_driver_sql("PRAGMA foreign_keys").scalar_one():
                raise RuntimeError(
                    "Cannot safely rebuild vendor_notes while SQLite foreign keys are enabled."
                )
            metadata = sa.MetaData()
            table = sa.Table("vendor_notes", metadata, autoload_with=bind)
            for constraint in list(table.constraints):
                if (
                    isinstance(constraint, sa.UniqueConstraint)
                    and tuple(column.name for column in constraint.columns) == ("note_no",)
                ):
                    table.constraints.remove(constraint)
            table.c.note_no.unique = False
            with op.batch_alter_table(
                "vendor_notes",
                recreate="always",
                copy_from=table,
            ) as batch_op:
                batch_op.create_unique_constraint(
                    "uq_vendor_notes_tenant_note_no",
                    ["tenant_id", "note_no"],
                )
            bind.exec_driver_sql("PRAGMA foreign_keys=ON")
            violations_after = bind.exec_driver_sql("PRAGMA foreign_key_check").all()
            if violations_after != violations_before:
                raise RuntimeError(
                    "Vendor-note migration changed pre-existing foreign-key violations."
                )
        elif not any(
            tuple(constraint.get("column_names") or ()) == ("tenant_id", "note_no")
            for constraint in inspector.get_unique_constraints("vendor_notes")
        ):
            op.create_index(
                "uq_vendor_notes_tenant_note_no",
                "vendor_notes",
                ["tenant_id", "note_no"],
                unique=True,
            )
        return

    for constraint in global_constraints:
        name = constraint.get("name")
        if not name:
            raise RuntimeError("Cannot safely identify the global vendor-note constraint.")
        op.drop_constraint(name, "vendor_notes", type_="unique")
    op.create_unique_constraint(
        "uq_vendor_notes_tenant_note_no",
        "vendor_notes",
        ["tenant_id", "note_no"],
    )


def downgrade():
    raise RuntimeError(
        "This migration cannot be safely downgraded after tenants may reuse note numbers."
    )
