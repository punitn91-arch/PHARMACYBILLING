"""Replace global vendor purchase numbering uniqueness with tenant scope.

Revision ID: 20261010_21
Revises: 20261009_20
"""

from alembic import op
import sqlalchemy as sa


revision = "20261010_21"
down_revision = "20261009_20"
branch_labels = None
depends_on = None


TABLE = "vendor_purchase"
TENANT_PURCHASE_INDEX = "uq_vendor_purchase_tenant_purchase_no"
TENANT_PURCHASE_COLUMNS = ("tenant_id", "purchase_no")


def _bind():
    return op.get_bind()


def _inspector():
    return sa.inspect(_bind())


def _has_table():
    return TABLE in _inspector().get_table_names()


def _assert_no_tenant_duplicates():
    duplicates = _bind().execute(
        sa.text(
            """
            SELECT tenant_id, purchase_no, COUNT(*)
            FROM vendor_purchase
            WHERE tenant_id IS NOT NULL AND purchase_no IS NOT NULL
            GROUP BY tenant_id, purchase_no
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )
    ).first()
    if duplicates:
        raise RuntimeError(
            "Cannot enforce tenant-scoped vendor purchase numbers: "
            "duplicate (tenant_id, purchase_no) records exist."
        )


def _sqlite_global_unique_constraints():
    inspector = _inspector()
    return [
        constraint
        for constraint in inspector.get_unique_constraints(TABLE)
        if tuple(constraint.get("column_names") or ()) == ("purchase_no",)
    ]


def _drop_sqlite_global_unique_constraints(constraints):
    if not constraints:
        return

    triggers = [
        row[0]
        for row in _bind().execute(
            sa.text(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'trigger' AND tbl_name = :table_name AND sql IS NOT NULL"
            ),
            {"table_name": TABLE},
        )
    ]
    names = {
        constraint.get("name") or "uq_vendor_purchase_purchase_no"
        for constraint in constraints
    }
    with op.batch_alter_table(
        TABLE,
        recreate="always",
        naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"},
    ) as batch:
        for name in names:
            batch.drop_constraint(name, type_="unique")
    for trigger_sql in triggers:
        op.execute(trigger_sql)


def _drop_global_unique_indexes():
    for index in _inspector().get_indexes(TABLE):
        if (
            index.get("unique")
            and tuple(index.get("column_names") or ()) == ("purchase_no",)
            and not index.get("duplicates_constraint")
        ):
            op.drop_index(index["name"], table_name=TABLE)


def _has_tenant_purchase_uniqueness():
    inspector = _inspector()
    if any(
        tuple(constraint.get("column_names") or ()) == TENANT_PURCHASE_COLUMNS
        for constraint in inspector.get_unique_constraints(TABLE)
    ):
        return True
    for index in inspector.get_indexes(TABLE):
        options = index.get("dialect_options") or {}
        if (
            index.get("unique")
            and tuple(index.get("column_names") or ()) == TENANT_PURCHASE_COLUMNS
            and options.get("sqlite_where") is None
            and options.get("postgresql_where") is None
        ):
            return True
    return False


def _ensure_tenant_purchase_uniqueness():
    indexes = {index["name"]: index for index in _inspector().get_indexes(TABLE)}
    existing = indexes.get(TENANT_PURCHASE_INDEX)
    if existing:
        if not (
            existing.get("unique")
            and tuple(existing.get("column_names") or ()) == TENANT_PURCHASE_COLUMNS
        ):
            raise RuntimeError(
                f"Index {TENANT_PURCHASE_INDEX} exists with an unexpected definition."
            )
        dialect_options = existing.get("dialect_options") or {}
        where = dialect_options.get("sqlite_where")
        if where is None:
            where = dialect_options.get("postgresql_where")
        if where is not None:
            predicate = str(where).lower()
            if (
                "tenant_id is not null" not in predicate
                or "purchase_no is not null" not in predicate
            ):
                raise RuntimeError(
                    f"Index {TENANT_PURCHASE_INDEX} has an incomplete predicate."
                )
        return
    if _has_tenant_purchase_uniqueness():
        return

    predicate = sa.text("tenant_id IS NOT NULL AND purchase_no IS NOT NULL")
    op.create_index(
        TENANT_PURCHASE_INDEX,
        TABLE,
        list(TENANT_PURCHASE_COLUMNS),
        unique=True,
        sqlite_where=predicate,
        postgresql_where=predicate,
    )


def upgrade():
    is_sqlite = _bind().dialect.name == "sqlite"
    foreign_keys_enabled = False
    if is_sqlite:
        foreign_keys_enabled = bool(
            _bind().exec_driver_sql("PRAGMA foreign_keys").scalar()
        )
        if foreign_keys_enabled:
            _bind().exec_driver_sql("PRAGMA foreign_keys=OFF")
    try:
        if not _has_table():
            return
        _assert_no_tenant_duplicates()

        if is_sqlite:
            constraints = _sqlite_global_unique_constraints()
            if constraints:
                _drop_sqlite_global_unique_constraints(constraints)

        _drop_global_unique_indexes()
        _ensure_tenant_purchase_uniqueness()
    finally:
        if is_sqlite and foreign_keys_enabled:
            _bind().exec_driver_sql("PRAGMA foreign_keys=ON")
            violations = _bind().exec_driver_sql("PRAGMA foreign_key_check").all()
            if violations:
                raise RuntimeError(
                    "Vendor purchase uniqueness migration left foreign key violations."
                )


def downgrade():
    if not _has_table():
        return
    cross_tenant_duplicates = _bind().execute(
        sa.text(
            """
            SELECT purchase_no, COUNT(DISTINCT tenant_id)
            FROM vendor_purchase
            WHERE tenant_id IS NOT NULL AND purchase_no IS NOT NULL
            GROUP BY purchase_no
            HAVING COUNT(DISTINCT tenant_id) > 1
            LIMIT 1
            """
        )
    ).first()
    if cross_tenant_duplicates:
        raise RuntimeError(
            "Cannot restore global vendor purchase number uniqueness: "
            "the same purchase_no exists in multiple tenants."
        )

    indexes = {index["name"]: index for index in _inspector().get_indexes(TABLE)}
    if TENANT_PURCHASE_INDEX in indexes:
        op.drop_index(TENANT_PURCHASE_INDEX, table_name=TABLE)
    if not any(
        tuple(constraint.get("column_names") or ()) == ("purchase_no",)
        for constraint in _inspector().get_unique_constraints(TABLE)
    ) and not any(
        index.get("unique")
        and tuple(index.get("column_names") or ()) == ("purchase_no",)
        for index in _inspector().get_indexes(TABLE)
    ):
        if _bind().dialect.name == "sqlite":
            with op.batch_alter_table(TABLE, recreate="always") as batch:
                batch.create_unique_constraint(
                    "uq_vendor_purchase_purchase_no_legacy", ["purchase_no"]
                )
        else:
            op.create_unique_constraint(
                "uq_vendor_purchase_purchase_no_legacy",
                TABLE,
                ["purchase_no"],
            )
