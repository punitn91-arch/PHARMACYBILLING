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
    "20261010_24_tenant_scoped_vendor_note_numbers.py",
)
SPEC = importlib.util.spec_from_file_location(
    "tenant_scoped_vendor_note_numbers_migration",
    MIGRATION_PATH,
)
MIGRATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIGRATION)


class VendorNoteNumberMigrationTests(unittest.TestCase):
    def test_sqlite_migration_preserves_references_and_scopes_note_number_uniqueness(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = sa.create_engine(f"sqlite:///{os.path.join(temp_dir, 'notes.db')}")
            with engine.begin() as connection:
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                connection.exec_driver_sql("CREATE TABLE tenant (id INTEGER PRIMARY KEY)")
                connection.exec_driver_sql("CREATE TABLE vendor (id INTEGER PRIMARY KEY)")
                connection.exec_driver_sql(
                    "CREATE TABLE vendor_purchase (id INTEGER PRIMARY KEY)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_notes (
                        id INTEGER PRIMARY KEY,
                        tenant_id INTEGER REFERENCES tenant(id),
                        note_no VARCHAR(40) NOT NULL UNIQUE,
                        note_type VARCHAR(10) NOT NULL,
                        vendor_id INTEGER NOT NULL REFERENCES vendor(id),
                        reference_purchase_id INTEGER REFERENCES vendor_purchase(id)
                    )
                    """
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_note_items (
                        id INTEGER PRIMARY KEY,
                        note_id INTEGER NOT NULL REFERENCES vendor_notes(id)
                    )
                    """
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_note_allocations (
                        id INTEGER PRIMARY KEY,
                        note_id INTEGER NOT NULL REFERENCES vendor_notes(id),
                        note_item_id INTEGER NOT NULL REFERENCES vendor_note_items(id)
                    )
                    """
                )
                connection.exec_driver_sql("INSERT INTO tenant VALUES (1), (2)")
                connection.exec_driver_sql("INSERT INTO vendor VALUES (1)")
                connection.exec_driver_sql("INSERT INTO vendor_purchase VALUES (1)")
                connection.exec_driver_sql(
                    "INSERT INTO vendor_notes VALUES (1, 1, 'VN-CN-000001', 'CREDIT', 1, 1)"
                )
                connection.exec_driver_sql(
                    "INSERT INTO vendor_note_items VALUES (1, 1)"
                )
                connection.exec_driver_sql(
                    "INSERT INTO vendor_note_allocations VALUES (1, 1, 1)"
                )

            with engine.begin() as connection:
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    MIGRATION.upgrade()

            with engine.begin() as connection:
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendor_notes"
                    ).scalar_one(),
                    1,
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT note_id FROM vendor_note_items"
                    ).scalar_one(),
                    1,
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT note_item_id FROM vendor_note_allocations"
                    ).scalar_one(),
                    1,
                )
                self.assertEqual(
                    connection.exec_driver_sql("PRAGMA foreign_key_check").fetchall(),
                    [],
                )
                connection.exec_driver_sql(
                    "INSERT INTO vendor_notes VALUES (2, 2, 'VN-CN-000001', 'CREDIT', 1, 1)"
                )
                with self.assertRaises(IntegrityError):
                    connection.exec_driver_sql(
                        "INSERT INTO vendor_notes VALUES (3, 1, 'VN-CN-000001', 'CREDIT', 1, 1)"
                    )
