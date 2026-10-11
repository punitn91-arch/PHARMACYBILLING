import importlib.util
import os
import tempfile
import unittest

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


MIGRATION_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "migrations",
    "versions",
    "20261010_22_tenant_scoped_identity_uniqueness.py",
)
SPEC = importlib.util.spec_from_file_location(
    "tenant_identity_uniqueness_migration", MIGRATION_PATH
)
MIGRATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIGRATION)


class TenantIdentityUniquenessMigrationTests(unittest.TestCase):
    def test_sqlite_migration_preserves_rows_triggers_and_scopes_identity_uniqueness(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "tenant_identity.db")
            engine = sa.create_engine(f"sqlite:///{database_path}")
            with engine.begin() as connection:
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                connection.exec_driver_sql(
                    "CREATE TABLE tenant (id INTEGER PRIMARY KEY)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE "user" (
                        id INTEGER PRIMARY KEY,
                        tenant_id INTEGER REFERENCES tenant(id),
                        username VARCHAR(50) NOT NULL UNIQUE,
                        email VARCHAR(100) NOT NULL UNIQUE
                    )
                    """
                )
                connection.exec_driver_sql(
                    "CREATE INDEX ix_user_tenant_id ON user (tenant_id)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE UNIQUE INDEX uq_user_tenant_username_ci
                    ON user (tenant_id, lower(username))
                    WHERE tenant_id IS NOT NULL AND username IS NOT NULL
                    """
                )
                connection.exec_driver_sql(
                    "CREATE TABLE patient (id INTEGER PRIMARY KEY, tenant_id INTEGER, mobile VARCHAR(20))"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_patient_mobile ON patient (mobile)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX uq_patient_tenant_mobile ON patient (tenant_id, mobile)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE appointment (id INTEGER PRIMARY KEY, tenant_id INTEGER, appointment_no VARCHAR(30))"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_appointment_appointment_no ON appointment (appointment_no)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX uq_appointment_tenant_appointment_no ON appointment (tenant_id, appointment_no)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE lab_test (id INTEGER PRIMARY KEY, tenant_id INTEGER, test_code VARCHAR(40))"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_lab_test_test_code ON lab_test (test_code)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX uq_lab_test_tenant_test_code ON lab_test (tenant_id, test_code)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE lab_order (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no VARCHAR(40))"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_lab_order_order_no ON lab_order (order_no)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX uq_lab_order_tenant_order_no ON lab_order (tenant_id, order_no)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE public_appointment_day_lock (id INTEGER PRIMARY KEY, tenant_id INTEGER, appointment_date DATE NOT NULL)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_public_appointment_day_lock_appointment_date ON public_appointment_day_lock (appointment_date)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE clinic_location (id INTEGER PRIMARY KEY, tenant_id INTEGER, code VARCHAR(40) NOT NULL)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_clinic_location_code ON clinic_location (code)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE clinician (id INTEGER PRIMARY KEY, tenant_id INTEGER, code VARCHAR(40) NOT NULL)"
                )
                connection.exec_driver_sql(
                    "CREATE UNIQUE INDEX ix_clinician_code ON clinician (code)"
                )
                connection.exec_driver_sql(
                    "CREATE TABLE user_audit (username TEXT NOT NULL)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE TRIGGER user_insert_audit AFTER INSERT ON user
                    BEGIN
                        INSERT INTO user_audit(username) VALUES (NEW.username);
                    END
                    """
                )
                connection.exec_driver_sql("INSERT INTO tenant (id) VALUES (1), (2)")
                connection.exec_driver_sql(
                    """
                    INSERT INTO user (id, tenant_id, username, email)
                    VALUES
                        (1, 1, 'Alpha', 'alpha@example.test'),
                        (2, 2, 'Beta', 'beta@example.test')
                    """
                )
                connection.exec_driver_sql(
                    "INSERT INTO patient (id, tenant_id, mobile) VALUES (1, 1, '111'), (2, 2, '222')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO appointment VALUES (1, 1, 'A-1'), (2, 2, 'A-2')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO lab_test VALUES (1, 1, 'T-1'), (2, 2, 'T-2')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO lab_order VALUES (1, 1, 'L-1'), (2, 2, 'L-2')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO public_appointment_day_lock VALUES (1, 1, '2026-10-10'), (2, 2, '2026-10-11')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO clinic_location VALUES (1, 1, 'MAIN-A'), (2, 2, 'MAIN-B')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO clinician VALUES (1, 1, 'DOC-A'), (2, 2, 'DOC-B')"
                )

            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    MIGRATION.upgrade()
                connection.commit()
                first_migration_user_sql = connection.exec_driver_sql(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='user'"
                ).scalar_one()
                self.assertNotRegex(
                    first_migration_user_sql.upper(),
                    r"USERNAME VARCHAR\(50\) NOT NULL UNIQUE",
                )
                self.assertIn(
                    "UNIQUE (email)",
                    first_migration_user_sql,
                )

            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    MIGRATION.upgrade()
                connection.commit()

            with engine.begin() as connection:
                user_sql = connection.exec_driver_sql(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='user'"
                ).scalar_one()
                self.assertNotRegex(
                    user_sql.upper(),
                    r"USERNAME VARCHAR\(50\) NOT NULL UNIQUE",
                )
                self.assertIn("UNIQUE (email)", user_sql)
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM user"
                    ).scalar_one(),
                    2,
                )
                user_unique_columns = []
                for index in connection.exec_driver_sql('PRAGMA index_list("user")'):
                    if index[2]:
                        user_unique_columns.append(
                            tuple(
                                row[2]
                                for row in connection.exec_driver_sql(
                                    'PRAGMA index_info("{}")'.format(index[1])
                                )
                            )
                        )
                self.assertIn(("email",), user_unique_columns)
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM patient"
                    ).scalar_one(),
                    2,
                )
                self.assertEqual(
                    connection.exec_driver_sql("PRAGMA foreign_key_check").all(), []
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM user_audit"
                    ).scalar_one(),
                    2,
                )

                connection.exec_driver_sql(
                    """
                    INSERT INTO user (tenant_id, username, email)
                    VALUES
                        (1, 'same', 'same-a@example.test'),
                        (2, 'same', 'same-b@example.test')
                    """
                )
                connection.exec_driver_sql(
                    "INSERT INTO patient (tenant_id, mobile) VALUES (1, '999'), (2, '999')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO appointment VALUES (3, 1, 'shared'), (4, 2, 'shared')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO lab_test VALUES (3, 1, 'shared'), (4, 2, 'shared')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO lab_order VALUES (3, 1, 'shared'), (4, 2, 'shared')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO public_appointment_day_lock VALUES (3, 1, '2026-10-12'), (4, 2, '2026-10-12')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO clinic_location VALUES (3, 1, 'SHARED'), (4, 2, 'SHARED')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO clinician VALUES (3, 1, 'SHARED'), (4, 2, 'SHARED')"
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM user_audit"
                    ).scalar_one(),
                    4,
                )

                for sql in (
                    "INSERT INTO user (tenant_id, username, email) VALUES (1, 'SAME', 'same-c@example.test')",
                    "INSERT INTO user (tenant_id, username, email) VALUES (1, 'other', 'alpha@example.test')",
                    "INSERT INTO patient (tenant_id, mobile) VALUES (1, '999')",
                    "INSERT INTO appointment VALUES (5, 1, 'shared')",
                    "INSERT INTO lab_test VALUES (5, 1, 'shared')",
                    "INSERT INTO lab_order VALUES (5, 1, 'shared')",
                    "INSERT INTO public_appointment_day_lock VALUES (5, 1, '2026-10-12')",
                    "INSERT INTO clinic_location VALUES (5, 1, 'SHARED')",
                    "INSERT INTO clinician VALUES (5, 1, 'SHARED')",
                ):
                    with self.subTest(sql=sql):
                        with self.assertRaises(IntegrityError):
                            with connection.begin_nested():
                                connection.exec_driver_sql(sql)

                indexes = sa.inspect(connection).get_indexes("patient")
                scoped = next(
                    index
                    for index in indexes
                    if index["name"] == "uq_patient_tenant_mobile"
                )
                self.assertIn(
                    "mobile <> ''",
                    str(scoped["dialect_options"]["sqlite_where"]),
                )
                self.assertNotIn("ix_patient_mobile", {index["name"] for index in indexes})


if __name__ == "__main__":
    unittest.main()
