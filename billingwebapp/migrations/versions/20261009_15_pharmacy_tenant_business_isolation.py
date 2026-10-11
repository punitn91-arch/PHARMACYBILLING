"""Tenant-scoped pharmacy business identifiers and file ownership notes.

Revision ID: 20261009_15
Revises: 20261009_14
Create Date: 2026-10-09
"""

from alembic import op
import sqlalchemy as sa


revision = "20261009_15"
down_revision = "20261009_14"
branch_labels = None
depends_on = None


GLOBAL_UNIQUE_COLUMNS = (
    ("user", "username"),
    ("invoice", "invoice_no"),
    ("return_bill", "return_no"),
    ("patient", "mobile"),
    ("lab_test", "test_code"),
    ("lab_order", "order_no"),
    ("lab_report", "storage_key"),
    ("vendor_purchase", "purchase_no"),
)


TENANT_UNIQUE_INDEXES = (
    ("uq_user_tenant_username_ci", "user", ("tenant_id", "lower(username)"), "tenant_id IS NOT NULL AND username IS NOT NULL"),
    ("uq_invoice_tenant_invoice_no", "invoice", ("tenant_id", "invoice_no"), "tenant_id IS NOT NULL AND invoice_no IS NOT NULL"),
    ("uq_return_tenant_return_no", "return_bill", ("tenant_id", "return_no"), "tenant_id IS NOT NULL AND return_no IS NOT NULL"),
    ("uq_patient_tenant_mobile", "patient", ("tenant_id", "mobile"), "tenant_id IS NOT NULL AND mobile IS NOT NULL AND mobile <> ''"),
    ("uq_vendor_purchase_tenant_purchase_no", "vendor_purchase", ("tenant_id", "purchase_no"), "tenant_id IS NOT NULL AND purchase_no IS NOT NULL"),
    ("uq_vendor_purchase_tenant_vendor_invoice", "vendor_purchase", ("tenant_id", "vendor_id", "lower(invoice_no)"), "tenant_id IS NOT NULL AND vendor_id IS NOT NULL AND invoice_no IS NOT NULL AND invoice_no <> ''"),
    ("uq_lab_test_tenant_test_code", "lab_test", ("tenant_id", "test_code"), "tenant_id IS NOT NULL AND test_code IS NOT NULL"),
    ("uq_lab_order_tenant_order_no", "lab_order", ("tenant_id", "order_no"), "tenant_id IS NOT NULL AND order_no IS NOT NULL"),
    ("uq_lab_report_tenant_storage_key", "lab_report", ("tenant_id", "storage_key"), "tenant_id IS NOT NULL AND storage_key IS NOT NULL"),
)


def _bind():
    return op.get_bind()


def _dialect():
    return _bind().dialect.name.lower()


def _inspector():
    return sa.inspect(_bind())


def _has_table(table):
    return table in _inspector().get_table_names()


def _indexes(table):
    if not _has_table(table):
        return set()
    return {idx["name"] for idx in _inspector().get_indexes(table)}


def _unique_constraints(table):
    if not _has_table(table):
        return []
    return _inspector().get_unique_constraints(table)


def _quote(name):
    return '"{}"'.format(name.replace('"', '""'))


def _drop_global_uniques():
    for table, column in GLOBAL_UNIQUE_COLUMNS:
        if not _has_table(table):
            continue
        for constraint in _unique_constraints(table):
            name = constraint.get("name")
            columns = tuple(constraint.get("column_names") or ())
            if columns == (column,) and name:
                with op.batch_alter_table(table) as batch:
                    batch.drop_constraint(name, type_="unique")


def _create_tenant_unique_indexes():
    for index_name, table, columns, predicate in TENANT_UNIQUE_INDEXES:
        if not _has_table(table) or index_name in _indexes(table):
            continue
        if _dialect() == "postgresql":
            cols_sql = ", ".join(columns)
            op.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" ON {_quote(table)} ({cols_sql}) WHERE {predicate}')
        else:
            plain_columns = [column for column in columns if "(" not in column]
            if len(plain_columns) == len(columns):
                op.create_index(index_name, table, plain_columns, unique=True)


def upgrade():
    _drop_global_uniques()
    _create_tenant_unique_indexes()


def downgrade():
    for index_name, table, _columns, _predicate in reversed(TENANT_UNIQUE_INDEXES):
        if _has_table(table) and index_name in _indexes(table):
            op.drop_index(index_name, table_name=table)
