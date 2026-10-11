import importlib.util
import os
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from unittest.mock import patch

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


MIGRATION_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "migrations",
    "versions",
    "20261010_21_vendor_purchase_tenant_number_unique.py",
)
SPEC = importlib.util.spec_from_file_location(
    "vendor_purchase_tenant_number_migration", MIGRATION_PATH
)
MIGRATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MIGRATION)


class VendorPurchaseNumberMigrationTests(unittest.TestCase):
    def test_sqlite_migration_preserves_data_references_and_supplier_bill_rule(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = os.path.join(temp_dir, "legacy_vendor_purchase.db")
            engine = sa.create_engine(f"sqlite:///{database_path}")
            with engine.begin() as connection:
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                connection.exec_driver_sql(
                    "CREATE TABLE tenant (id INTEGER PRIMARY KEY)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_purchase (
                        id INTEGER PRIMARY KEY,
                        tenant_id INTEGER REFERENCES tenant(id),
                        vendor_id INTEGER NOT NULL,
                        purchase_no VARCHAR(30),
                        invoice_no VARCHAR(60),
                        UNIQUE (purchase_no),
                        UNIQUE (tenant_id, vendor_id, invoice_no)
                    )
                    """
                )
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_purchase_item (
                        id INTEGER PRIMARY KEY,
                        purchase_id INTEGER NOT NULL REFERENCES vendor_purchase(id)
                    )
                    """
                )
                connection.exec_driver_sql(
                    "CREATE TABLE purchase_audit (purchase_id INTEGER NOT NULL)"
                )
                connection.exec_driver_sql(
                    """
                    CREATE TRIGGER vendor_purchase_audit_insert
                    AFTER INSERT ON vendor_purchase
                    BEGIN
                        INSERT INTO purchase_audit (purchase_id) VALUES (NEW.id);
                    END
                    """
                )
                connection.exec_driver_sql("INSERT INTO tenant (id) VALUES (1), (2)")
                connection.exec_driver_sql(
                    """
                    INSERT INTO vendor_purchase (id, tenant_id, vendor_id, purchase_no, invoice_no)
                    VALUES (1, 1, 10, 'PB-00001', 'SUP-001'),
                           (2, 2, 20, 'PB-00002', 'SUP-002')
                    """
                )
                connection.exec_driver_sql(
                    "INSERT INTO vendor_purchase_item (id, purchase_id) VALUES (1, 1)"
                )
                connection.exec_driver_sql(
                    "INSERT INTO purchase_audit (purchase_id) VALUES (0)"
                )

            with engine.begin() as connection:
                with self.assertRaises(IntegrityError):
                    with connection.begin_nested():
                        connection.exec_driver_sql(
                            """
                            INSERT INTO vendor_purchase (tenant_id, vendor_id, purchase_no, invoice_no)
                            VALUES (2, 20, 'PB-00001', 'SUP-003')
                            """
                        )

            with engine.connect() as connection:
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    MIGRATION.upgrade()
                connection.commit()

            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    MIGRATION.upgrade()
                connection.commit()

            with engine.connect() as connection:
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendor_purchase"
                    ).scalar_one(),
                    2,
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT id, purchase_no FROM vendor_purchase ORDER BY id"
                    ).all(),
                    [(1, "PB-00001"), (2, "PB-00002")],
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT purchase_id FROM vendor_purchase_item"
                    ).scalar_one(),
                    1,
                )
                self.assertEqual(
                    connection.exec_driver_sql("PRAGMA foreign_key_check").all(), []
                )
                foreign_keys = connection.exec_driver_sql(
                    "PRAGMA foreign_key_list(vendor_purchase_item)"
                ).all()
                self.assertEqual(foreign_keys[0][2], "vendor_purchase")
                self.assertIsNotNone(
                    connection.exec_driver_sql(
                        """
                        SELECT sql FROM sqlite_master
                        WHERE type='trigger' AND name='vendor_purchase_audit_insert'
                        """
                    ).scalar_one()
                )
                purchase_indexes = sa.inspect(connection).get_indexes(
                    "vendor_purchase"
                )
                self.assertFalse(
                    any(
                        index.get("unique")
                        and index.get("column_names") == ["purchase_no"]
                        for index in purchase_indexes
                    )
                )
                self.assertTrue(
                    any(
                        index.get("unique")
                        and index.get("column_names") == ["tenant_id", "purchase_no"]
                        for index in purchase_indexes
                    )
                )
                self.assertTrue(
                    any(
                        constraint.get("column_names")
                        == ["tenant_id", "vendor_id", "invoice_no"]
                        for constraint in sa.inspect(connection).get_unique_constraints(
                            "vendor_purchase"
                        )
                    )
                )

                connection.exec_driver_sql(
                    """
                    INSERT INTO vendor_purchase (tenant_id, vendor_id, purchase_no, invoice_no)
                    VALUES (2, 10, 'PB-00001', 'SUP-001')
                    """
                )
                connection.commit()
                with self.assertRaises(IntegrityError):
                    with connection.begin_nested():
                        connection.exec_driver_sql(
                            """
                            INSERT INTO vendor_purchase (tenant_id, vendor_id, purchase_no, invoice_no)
                            VALUES (1, 10, 'PB-00001', 'SUP-004')
                            """
                        )
                with self.assertRaises(IntegrityError):
                    with connection.begin_nested():
                        connection.exec_driver_sql(
                            """
                            INSERT INTO vendor_purchase (tenant_id, vendor_id, purchase_no, invoice_no)
                            VALUES (1, 10, 'PB-00003', 'SUP-001')
                            """
                        )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM purchase_audit"
                    ).scalar_one(),
                    4,
                )

            engine.dispose()

    def test_migration_refuses_existing_same_tenant_duplicates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = sa.create_engine(
                f"sqlite:///{os.path.join(temp_dir, 'duplicate_vendor_purchase.db')}"
            )
            with engine.begin() as connection:
                connection.exec_driver_sql(
                    """
                    CREATE TABLE vendor_purchase (
                        id INTEGER PRIMARY KEY,
                        tenant_id INTEGER,
                        purchase_no VARCHAR(30)
                    )
                    """
                )
                connection.exec_driver_sql(
                    """
                    INSERT INTO vendor_purchase (tenant_id, purchase_no)
                    VALUES (1, 'PB-00001'), (1, 'PB-00001')
                    """
                )
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                with Operations.context(context):
                    with self.assertRaisesRegex(RuntimeError, "duplicate"):
                        MIGRATION.upgrade()
                connection.rollback()
            with engine.connect() as connection:
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendor_purchase"
                    ).scalar_one(),
                    2,
                )
                self.assertFalse(
                    any(
                        index.get("unique")
                        and index.get("column_names") == ["tenant_id", "purchase_no"]
                        for index in sa.inspect(connection).get_indexes("vendor_purchase")
                    )
                )
            engine.dispose()


class VendorPurchaseNumberSaveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "purchase_workflow.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["SECRET_KEY"] = "vendor-purchase-number-test"
        os.environ["DEFAULT_ADMIN_PASSWORD"] = "test-bootstrap-password-not-used"
        os.environ["CSRF_PROTECTION"] = "0"
        os.environ["APP_TIMEZONE"] = "Asia/Kolkata"
        os.environ["ENABLE_BACKGROUND_JOBS"] = "0"
        os.environ["APP_STORAGE_ROOT"] = os.path.join(cls.temp_dir.name, "uploads")
        os.environ["APP_BACKUP_ROOT"] = os.path.join(cls.temp_dir.name, "backups")
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_dir not in sys.path:
            sys.path.insert(0, project_dir)
        if "app" in sys.modules:
            cls.app_module = importlib.reload(sys.modules["app"])
        else:
            cls.app_module = importlib.import_module("app")
        cls.app = cls.app_module.app
        cls.db = cls.app_module.db
        cls.app.config["TESTING"] = True

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def setUp(self):
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()

    def test_default_flask_database_path_targets_populated_business_database(self):
        expected_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "instance",
                "pharmacy.db",
            )
        )
        self.assertEqual(
            sa.engine.make_url(self.app_module.default_db).database,
            expected_path,
        )
        with self.app.app_context():
            self.assertEqual(
                str(self.app_module.db.engine.url),
                os.environ["DATABASE_URL"],
            )

    def test_legacy_integer_active_medicine_insert_is_tenant_scoped_and_returns_saved_row(self):
        tenant_id, _vendor_id = self._seed_tenant("legacy")
        with self.app.app_context(), self.app_module.tenant_context(tenant_id):
            medicine = self.app_module.Medicine(
                tenant_id=tenant_id + 100,
                name="LEGACY INTEGER MEDICINE",
                batch="LEGACY-001",
                expiry="2027-12-31",
                mrp=25,
                qty=0,
                gst_percent=12.5,
                is_active=True,
            )
            with patch.object(
                self.app_module,
                "runtime_boolean_storage_mode",
                return_value="integer",
            ):
                persisted = self.app_module.persist_new_medicine_record(medicine)

            self.assertIsNotNone(persisted.id)
            self.assertGreater(persisted.id, 0)
            self.assertEqual(persisted.tenant_id, tenant_id)
            row = self.db.session.execute(
                sa.text(
                    "SELECT tenant_id, gst_percent, is_active, typeof(is_active) "
                    "FROM medicine WHERE id = :medicine_id"
                ),
                {"medicine_id": persisted.id},
            ).one()
            self.assertEqual(row.tenant_id, tenant_id)
            self.assertEqual(row.gst_percent, 12.5)
            self.assertEqual(row.is_active, 1)
            self.assertEqual(row[3], "integer")

    def test_failed_new_medicine_persistence_rolls_back_purchase(self):
        tenant_id, vendor_id = self._seed_tenant("rollback")
        client = self._login("rollback")
        with patch.object(
            self.app_module,
            "persist_new_medicine_record",
            side_effect=RuntimeError("forced medicine persistence failure"),
        ):
            response = self._purchase_request(
                client,
                vendor_id,
                "SUP-ROLLBACK",
                "ROLLBACK-LOT",
                qty=2,
            )

        self.assertEqual(response.status_code, 500)
        with self.app.app_context(), self.app_module.tenant_context(tenant_id):
            self.assertEqual(self.app_module.VendorPurchase.query.count(), 0)
            self.assertIsNone(
                self.app_module.Medicine.query.filter_by(batch="ROLLBACK-LOT").first()
            )
            self.assertEqual(
                self.app_module.StockHistory.query.filter_by(
                    batch="ROLLBACK-LOT"
                ).count(),
                0,
            )

    def test_purchase_number_continues_from_existing_legacy_tenant_series(self):
        tenant_id, vendor_id = self._seed_tenant("legacyseries")
        with self.app.app_context(), self.app_module.tenant_scope_disabled():
            self.db.session.add_all([
                self.app_module.VendorPurchase(
                    tenant_id=tenant_id,
                    vendor_id=vendor_id,
                    purchase_no="PB00075",
                    invoice_no="OLD-SUP-075",
                    purchase_date=datetime(2026, 1, 1),
                ),
                self.app_module.VendorPurchase(
                    tenant_id=tenant_id,
                    vendor_id=vendor_id,
                    purchase_no="PB-00076",
                    invoice_no="OLD-SUP-076",
                    purchase_date=datetime(2026, 1, 2),
                ),
            ])
            self.db.session.commit()

        client = self._login("legacyseries")
        response = self._purchase_request(
            client,
            vendor_id,
            "SUP-NEXT-077",
            "LEGACY-NEXT-077",
            qty=1,
        )
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertEqual(response.get_json()["purchase_no"], "PB-00077")

    def _seed_tenant(self, code):
        with self.app.app_context(), self.app_module.tenant_scope_disabled():
            settings = self.app_module.TenantSettings(settings_json="{}")
            tenant = self.app_module.Tenant(
                tenant_uid=self.app_module.generate_tenant_uid(),
                tenant_code=code,
                pharmacy_name=f"{code.title()} Pharmacy",
                subscription_plan_ref="test",
                status="Active",
                settings=settings,
            )
            self.db.session.add_all([settings, tenant])
            self.db.session.flush()
            user = self.app_module.User(
                tenant_id=tenant.id,
                username=f"{code}_admin",
                role="admin",
                access_profile="admin",
                can_manage_users=True,
                can_view_reports=True,
                can_invoice_action=True,
                can_edit_invoice=True,
                can_delete_invoice=True,
                can_view_medicine=True,
                can_add_medicine=True,
                can_edit_medicine=True,
                can_delete_medicine=True,
                can_view_stock_history=True,
                can_manage_purchases=True,
                can_view_audit_logs=True,
                can_view_profit_dashboard=True,
            )
            user.set_password("Admin@123")
            vendor = self.app_module.Vendor(
                tenant_id=tenant.id,
                name=f"{code.title()} Distributor",
                is_active=True,
            )
            medicine = self.app_module.Medicine(
                tenant_id=tenant.id,
                name="PARACETAMOL 650",
                batch=f"{code.upper()}-BASE",
                expiry="2027-12-31",
                mrp=25,
                qty=10,
                is_active=True,
                pack_type="Box",
                pack_qty=1,
            )
            self.db.session.add_all([user, vendor, medicine])
            self.db.session.commit()
            return tenant.id, vendor.id

    def _login(self, tenant_code):
        client = self.app.test_client()
        response = client.post(
            "/login",
            data={
                "tenant": tenant_code,
                "username": f"{tenant_code}_admin",
                "password": "Admin@123",
            },
        )
        self.assertEqual(response.status_code, 302)
        return client

    def _purchase_request(self, client, vendor_id, invoice_no, batch, qty=2, free_qty=0):
        items = [
            {
                "medicine_name": "PARACETAMOL 650",
                "batch": batch,
                "expiry": "12/2027",
                "pack_type": "Box",
                "pack_qty": 1,
                "qty": qty,
                "free_qty": free_qty,
                "purchase_rate": 10,
                "mrp": 25,
                "gst_percent": 12,
                "discount_percent": 5,
            }
        ]
        return client.post(
            f"/vendor/{vendor_id}/purchase",
            data={
                "invoice_no": invoice_no,
                "purchase_date": datetime.now().date().isoformat(),
                "payment_mode": "CASH",
                "payment_status": "Paid",
                "paid_amount": "0",
                "purchase_items_json": self.app_module.json.dumps(items),
            },
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json",
            },
        )

    def test_tenant_numbering_duplicates_concurrency_and_stock_integrity(self):
        alpha_id, alpha_vendor_id = self._seed_tenant("alpha")
        beta_id, beta_vendor_id = self._seed_tenant("beta")
        alpha_client = self._login("alpha")
        beta_client = self._login("beta")

        alpha_response = self._purchase_request(
            alpha_client, alpha_vendor_id, "SUP-001", "ALPHA-LOT-1", qty=2, free_qty=1
        )
        beta_response = self._purchase_request(
            beta_client, beta_vendor_id, "SUP-001", "BETA-LOT-1", qty=2, free_qty=1
        )
        self.assertEqual(alpha_response.status_code, 200, alpha_response.get_data(as_text=True))
        self.assertEqual(beta_response.status_code, 200, beta_response.get_data(as_text=True))
        self.assertEqual(alpha_response.get_json()["purchase_no"], "PB-00001")
        self.assertEqual(beta_response.get_json()["purchase_no"], "PB-00001")
        self.assertAlmostEqual(alpha_response.get_json()["totals"]["gst_total"], 2.28)
        self.assertAlmostEqual(alpha_response.get_json()["totals"]["total_amount"], 21.28)
        self.assertEqual(
            alpha_response.get_json()["redirect_url"],
            f"/vendor/edit/{alpha_vendor_id}",
        )
        history_response = alpha_client.get(
            alpha_response.get_json()["redirect_url"]
            + "?purchase_saved=1&purchase_no=PB-00001"
        )
        self.assertEqual(history_response.status_code, 200)
        self.assertIn(b"SUP-001", history_response.data)
        self.assertIn(b"Vendor Bill History", history_response.data)

        concurrent_clients = [self._login("alpha"), self._login("alpha")]
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(
                executor.map(
                    lambda pair: self._purchase_request(
                        pair[0],
                        alpha_vendor_id,
                        pair[1],
                        pair[2],
                        qty=1,
                    ),
                    zip(
                        concurrent_clients,
                        ("SUP-002", "SUP-003"),
                        ("ALPHA-LOT-2", "ALPHA-LOT-3"),
                    ),
                )
            )
        self.assertTrue(
            all(response.status_code == 200 for response in responses),
            [response.get_data(as_text=True) for response in responses],
        )

        with self.app.app_context(), self.app_module.tenant_context(alpha_id):
            alpha_purchases = self.app_module.VendorPurchase.query.order_by(
                self.app_module.VendorPurchase.id
            ).all()
            self.assertEqual(
                [purchase.purchase_no for purchase in alpha_purchases],
                ["PB-00001", "PB-00002", "PB-00003"],
            )
            with self.assertRaises(IntegrityError):
                self.db.session.add(
                    self.app_module.VendorPurchase(
                        tenant_id=alpha_id,
                        vendor_id=alpha_vendor_id,
                        purchase_no="PB-00001",
                        invoice_no="SUP-DUPLICATE-NUMBER",
                    )
                )
                self.db.session.commit()
            self.db.session.rollback()
            alpha_stock = sum(
                row.qty
                for row in self.app_module.Medicine.query.filter_by(
                    name="PARACETAMOL 650"
                ).all()
            )
            alpha_item_count = self.app_module.VendorPurchaseItem.query.count()
            self.assertEqual(alpha_stock, 15)
            self.assertEqual(alpha_item_count, 3)
            alpha_first_item = self.app_module.VendorPurchaseItem.query.filter_by(
                purchase_id=alpha_purchases[0].id
            ).one()
            alpha_medicine = self.app_module.Medicine.query.filter_by(
                id=alpha_first_item.medicine_id,
                tenant_id=alpha_id,
            ).one()
            alpha_history = self.app_module.StockHistory.query.filter_by(
                medicine_id=alpha_medicine.id,
                action="PURCHASE",
            ).one()
            self.assertEqual(alpha_first_item.medicine_id, alpha_medicine.id)
            self.assertEqual(alpha_first_item.tenant_id, alpha_id)
            self.assertEqual(alpha_history.tenant_id, alpha_id)
            self.assertEqual(alpha_history.qty_change, 3)
            self.assertEqual(alpha_history.stock_before, 0)
            self.assertEqual(alpha_history.stock_after, 3)

        with self.app.app_context(), self.app_module.tenant_context(beta_id):
            beta_purchase = self.app_module.VendorPurchase.query.one()
            self.assertEqual(beta_purchase.purchase_no, "PB-00001")
            beta_stock = sum(
                row.qty
                for row in self.app_module.Medicine.query.filter_by(
                    name="PARACETAMOL 650"
                ).all()
            )
            self.assertEqual(beta_stock, 13)


if __name__ == "__main__":
    unittest.main()
