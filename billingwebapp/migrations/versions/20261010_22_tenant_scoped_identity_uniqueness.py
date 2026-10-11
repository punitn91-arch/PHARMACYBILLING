"""Remove unintended global uniqueness from tenant-owned identities.

Revision ID: 20261010_22
Revises: 20261010_21
"""

from alembic import op
import sqlalchemy as sa


revision = "20261010_22"
down_revision = "20261010_21"
branch_labels = None
depends_on = None


SCOPED_INDEXES = (
    (
        "uq_patient_tenant_mobile",
        "patient",
        ("tenant_id", "mobile"),
        "tenant_id IS NOT NULL AND mobile IS NOT NULL AND mobile <> ''",
    ),
    (
        "uq_user_tenant_username_ci",
        "user",
        ("tenant_id", sa.text("lower(username)")),
        "tenant_id IS NOT NULL AND username IS NOT NULL",
    ),
    (
        "uq_user_platform_username_ci",
        "user",
        (sa.text("lower(username)"),),
        "tenant_id IS NULL AND username IS NOT NULL",
    ),
    (
        "uq_appointment_tenant_appointment_no",
        "appointment",
        ("tenant_id", "appointment_no"),
        "tenant_id IS NOT NULL AND appointment_no IS NOT NULL",
    ),
    (
        "uq_lab_test_tenant_test_code",
        "lab_test",
        ("tenant_id", "test_code"),
        "tenant_id IS NOT NULL AND test_code IS NOT NULL",
    ),
    (
        "uq_lab_order_tenant_order_no",
        "lab_order",
        ("tenant_id", "order_no"),
        "tenant_id IS NOT NULL AND order_no IS NOT NULL",
    ),
    (
        "uq_public_appointment_day_lock_tenant_date",
        "public_appointment_day_lock",
        ("tenant_id", "appointment_date"),
        "tenant_id IS NOT NULL AND appointment_date IS NOT NULL",
    ),
    (
        "uq_clinic_location_tenant_code",
        "clinic_location",
        ("tenant_id", "code"),
        "tenant_id IS NOT NULL AND code IS NOT NULL",
    ),
    (
        "uq_clinician_tenant_code",
        "clinician",
        ("tenant_id", "code"),
        "tenant_id IS NOT NULL AND code IS NOT NULL",
    ),
)

GLOBAL_INDEXES_TO_DROP = (
    ("ix_patient_mobile", "patient"),
    ("ix_appointment_appointment_no", "appointment"),
    ("ix_lab_test_test_code", "lab_test"),
    ("ix_lab_order_order_no", "lab_order"),
    ("ix_public_appointment_day_lock_appointment_date", "public_appointment_day_lock"),
    ("ix_clinic_location_code", "clinic_location"),
    ("ix_clinician_code", "clinician"),
)

GLOBAL_UNIQUE_CONSTRAINT_COLUMNS = {
    "user": "username",
    "patient": "mobile",
    "appointment": "appointment_no",
    "lab_test": "test_code",
    "lab_order": "order_no",
    "public_appointment_day_lock": "appointment_date",
    "clinic_location": "code",
    "clinician": "code",
}


def _bind():
    return op.get_bind()


def _inspector():
    return sa.inspect(_bind())


def _tables():
    return set(_inspector().get_table_names())


def _indexes(table):
    if table not in _tables():
        return set()
    names = {index["name"] for index in _inspector().get_indexes(table)}
    if _bind().dialect.name == "sqlite":
        names.update(
            row[0]
            for row in _bind().execute(
                sa.text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'index' AND tbl_name = :table_name"
                ),
                {"table_name": table},
            )
        )
    return names


def _assert_no_duplicates(table, columns, where, label):
    if table not in _tables():
        return
    column_sql = ", ".join(columns)
    duplicate = _bind().execute(
        sa.text(
            f"SELECT {column_sql}, COUNT(*) FROM {table} "
            f"WHERE {where} GROUP BY {column_sql} HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicate:
        raise RuntimeError(
            "Cannot apply tenant-scoped uniqueness migration: duplicate "
            f"{label} values already exist."
        )


def _check_duplicates():
    _assert_no_duplicates(
        "patient",
        ("tenant_id", "mobile"),
        "tenant_id IS NOT NULL AND mobile IS NOT NULL AND mobile <> ''",
        "patient mobile",
    )
    _assert_no_duplicates(
        "user",
        ("tenant_id", "lower(username)"),
        "tenant_id IS NOT NULL AND username IS NOT NULL",
        "tenant username",
    )
    _assert_no_duplicates(
        "user",
        ("lower(username)",),
        "tenant_id IS NULL AND username IS NOT NULL",
        "platform username",
    )
    for table, column, label in (
        ("appointment", "appointment_no", "appointment number"),
        ("lab_test", "test_code", "lab test code"),
        ("lab_order", "order_no", "lab order number"),
        ("public_appointment_day_lock", "appointment_date", "appointment date lock"),
        ("clinic_location", "code", "clinic location code"),
        ("clinician", "code", "clinician code"),
    ):
        _assert_no_duplicates(
            table,
            ("tenant_id", column),
            f"tenant_id IS NOT NULL AND {column} IS NOT NULL",
            label,
        )


def _sqlite_unique_table_constraint(table, column):
    quoted_table = table.replace('"', '""')
    for _sequence, name, is_unique, origin, _partial in _bind().exec_driver_sql(
        f'PRAGMA index_list("{quoted_table}")'
    ):
        if not is_unique or origin != "u":
            continue
        quoted_name = name.replace('"', '""')
        columns = [
            row[2]
            for row in _bind().exec_driver_sql(
                f'PRAGMA index_info("{quoted_name}")'
            )
        ]
        if columns == [column]:
            return {"name": None, "column_names": columns}
    return None


def _rebuild_sqlite_table_without_global_unique(table, column):
    if _bind().dialect.name != "sqlite" or table not in _tables():
        return
    if not _sqlite_unique_table_constraint(table, column):
        return

    if any(
        foreign_key.get("referred_table") == table
        for dependent_table in _tables() - {table}
        for foreign_key in _inspector().get_foreign_keys(dependent_table)
    ):
        raise RuntimeError(
            f"Cannot safely rebuild SQLite table {table}: another table references it."
        )

    metadata = sa.MetaData()
    source = sa.Table(table, metadata, autoload_with=_bind())
    rebuilt_metadata = sa.MetaData()
    for foreign_key in source.foreign_keys:
        referenced_table = foreign_key.column.table.name
        if referenced_table not in rebuilt_metadata.tables:
            sa.Table(referenced_table, rebuilt_metadata, autoload_with=_bind())
    temporary_table = f"_{table}_tenant_identity_rebuild"
    rebuilt = source.to_metadata(rebuilt_metadata, name=temporary_table)
    sqlite_table_uniques = []
    quoted_table = table.replace('"', '""')
    for index_row in _bind().exec_driver_sql(f'PRAGMA index_list("{quoted_table}")'):
        if index_row[3] != "u":
            continue
        quoted_index = index_row[1].replace('"', '""')
        columns = tuple(
            row[2]
            for row in _bind().exec_driver_sql(f'PRAGMA index_info("{quoted_index}")')
        )
        if columns and all(columns) and columns != (column,):
            sqlite_table_uniques.append(columns)
    existing_unique_columns = {
        tuple(item.name for item in constraint.columns)
        for constraint in rebuilt.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    }
    for constraint in list(rebuilt.constraints):
        if (
            isinstance(constraint, sa.UniqueConstraint)
            and tuple(item.name for item in constraint.columns) == (column,)
        ):
            rebuilt.constraints.remove(constraint)
    for columns in sqlite_table_uniques:
        if columns not in existing_unique_columns:
            rebuilt.append_constraint(sa.UniqueConstraint(*columns))

    objects = _bind().execute(
        sa.text(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE tbl_name = :table AND type IN ('index', 'trigger') AND sql IS NOT NULL"
        ),
        {"table": table},
    ).all()
    indexes_to_restore = []
    for object_type, name, create_sql in objects:
        if object_type == "trigger":
            continue
        quoted_name = name.replace('"', '""')
        index_rows = _bind().exec_driver_sql(f'PRAGMA index_list("{quoted_table}")').all()
        index_row = next((row for row in index_rows if row[1] == name), None)
        if index_row and index_row[2]:
            columns = [
                row[2]
                for row in _bind().exec_driver_sql(
                    f'PRAGMA index_info("{quoted_name}")'
                )
            ]
            if columns == [column]:
                continue
        indexes_to_restore.append(create_sql)
    triggers_to_restore = [
        create_sql
        for object_type, _name, create_sql in objects
        if object_type == "trigger"
    ]

    preparer = _bind().dialect.identifier_preparer
    columns_sql = ", ".join(preparer.quote(item.name) for item in source.columns)
    _bind().exec_driver_sql(
        str(sa.schema.CreateTable(rebuilt).compile(dialect=_bind().dialect))
    )
    _bind().exec_driver_sql(
        f'INSERT INTO "{temporary_table}" ({columns_sql}) '
        f'SELECT {columns_sql} FROM "{table}"'
    )
    quoted_table = table.replace('"', '""')
    _bind().exec_driver_sql(f'DROP TABLE "{quoted_table}"')
    _bind().exec_driver_sql(
        f'ALTER TABLE "{temporary_table}" RENAME TO "{quoted_table}"'
    )
    for create_sql in indexes_to_restore + triggers_to_restore:
        _bind().exec_driver_sql(create_sql)


def _drop_global_unique_constraints():
    for table, target_column in GLOBAL_UNIQUE_CONSTRAINT_COLUMNS.items():
        if table not in _tables():
            continue
        for constraint in _inspector().get_unique_constraints(table):
            columns = tuple(constraint.get("column_names") or ())
            if columns != (target_column,):
                continue
            name = constraint.get("name")
            if name and _bind().dialect.name != "sqlite":
                op.drop_constraint(name, table, type_="unique")
    for name, table in GLOBAL_INDEXES_TO_DROP:
        if name in _indexes(table):
            op.drop_index(name, table_name=table)


def _create_scoped_indexes():
    for name, table, columns, predicate in SCOPED_INDEXES:
        if table not in _tables():
            continue
        if name in _indexes(table):
            if name != "uq_patient_tenant_mobile":
                continue
            op.drop_index(name, table_name=table)
        op.create_index(
            name,
            table,
            list(columns),
            unique=True,
            sqlite_where=sa.text(predicate),
            postgresql_where=sa.text(predicate),
        )


def upgrade():
    sqlite = _bind().dialect.name == "sqlite"
    violations_before = (
        _bind().exec_driver_sql("PRAGMA foreign_key_check").all()
        if sqlite
        else []
    )
    _check_duplicates()
    _rebuild_sqlite_table_without_global_unique("user", "username")
    _drop_global_unique_constraints()
    _create_scoped_indexes()
    if sqlite:
        violations_after = _bind().exec_driver_sql("PRAGMA foreign_key_check").all()
        if violations_after != violations_before:
            raise RuntimeError(
                "Tenant identity migration changed pre-existing foreign-key violations."
            )


def downgrade():
    raise RuntimeError(
        "This migration cannot be safely downgraded after tenants may reuse "
        "business identifiers."
    )
