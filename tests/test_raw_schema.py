"""Tests for Step 3.3.2 — PostgreSQL Raw Schema & Ingestion Foundation.

These tests exercise the *structure* of the `raw` schema against a live
PostgreSQL instance (via information_schema / pg_catalog). They do NOT load
any of the real source CSVs — grain, row counts, and data content are out of
scope here; that belongs to the ingestion pipeline (Step 3.3.3+).

If no PostgreSQL instance is reachable (missing env config, driver not
installed, or connection refused), every test in this module is SKIPPED
rather than failed, and reports why. This lets the suite run cleanly in
environments (like CI runners without Docker, or this sandbox) where a
database isn't available, while still being fully meaningful wherever
Postgres *is* up (e.g. `docker compose up -d` locally).
"""

from __future__ import annotations

import os
import unittest

from database.connection import (
    DatabaseConfigError,
    DatabaseConnectionError,
    close_connection,
    get_connection,
)
from database.init_db import init_raw_schema

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EXPECTED_METADATA_COLUMNS = {
    "ingestion_timestamp",
    "source_file",
    "batch_id",
    "ingestion_run_id",
}

# dataset_name -> expected source columns (from the approved YAML contracts).
EXPECTED_SOURCE_COLUMNS = {
    "users": {"user_id", "name", "email", "gender", "city", "signup_date"},
    "products": {"product_id", "product_name", "category", "brand", "price", "rating"},
    "orders": {"order_id", "user_id", "order_date", "order_status", "total_amount"},
    "order_items": {
        "order_item_id", "order_id", "product_id", "user_id",
        "quantity", "item_price", "item_total",
    },
    "reviews": {
        "review_id", "order_id", "product_id", "user_id",
        "rating", "review_text", "review_date",
    },
    "events": {"event_id", "user_id", "product_id", "event_type", "event_timestamp"},
}

RAW_TABLES = list(EXPECTED_SOURCE_COLUMNS.keys())


def _try_connect():
    """Return a live connection, or None if the DB isn't reachable.

    Used by setUpClass to decide whether to skip the whole module.
    """
    try:
        conn = get_connection()
    except (DatabaseConfigError, DatabaseConnectionError):
        return None
    return conn


class RawSchemaTestBase(unittest.TestCase):
    """Common setup: connect once, skip everything if unavailable."""

    conn = None

    @classmethod
    def setUpClass(cls):
        cls.conn = _try_connect()
        if cls.conn is None:
            raise unittest.SkipTest(
                "No reachable PostgreSQL instance (check POSTGRES_HOST / "
                "POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / "
                "POSTGRES_PASSWORD and that Docker/Postgres is running). "
                "Raw-schema structural tests are skipped, not failed, in "
                "this environment."
            )
        # Ensure the schema exists before asserting against it.
        init_raw_schema()

    @classmethod
    def tearDownClass(cls):
        close_connection(cls.conn)

    def _columns(self, table_name: str) -> dict[str, str]:
        """Return {column_name: data_type} for raw.<table_name>."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = %s
                """,
                (table_name,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}


class TestRawSchemaAndTablesExist(RawSchemaTestBase):
    def test_raw_schema_exists(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'raw'"
            )
            self.assertIsNotNone(cur.fetchone(), "raw schema does not exist")

    def test_all_six_raw_tables_exist(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'raw'
                """
            )
            existing = {row[0] for row in cur.fetchall()}
        for table in RAW_TABLES:
            with self.subTest(table=table):
                self.assertIn(table, existing)

    def test_ingestion_audit_exists(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'raw' AND table_name = 'ingestion_audit'
                """
            )
            self.assertIsNotNone(cur.fetchone())


class TestRawTableColumns(RawSchemaTestBase):
    def test_expected_source_columns_exist(self):
        for table, expected_cols in EXPECTED_SOURCE_COLUMNS.items():
            with self.subTest(table=table):
                actual = set(self._columns(table).keys())
                missing = expected_cols - actual
                self.assertFalse(missing, f"{table} missing columns: {missing}")

    def test_metadata_columns_exist_on_all_six_tables(self):
        for table in RAW_TABLES:
            with self.subTest(table=table):
                actual = set(self._columns(table).keys())
                missing = EXPECTED_METADATA_COLUMNS - actual
                self.assertFalse(
                    missing, f"{table} missing metadata columns: {missing}"
                )

    def test_ingestion_audit_expected_columns(self):
        expected = {
            "ingestion_run_id", "batch_id", "dataset_name", "source_file",
            "file_checksum", "started_at", "completed_at", "duration_seconds",
            "rows_read", "rows_loaded", "validation_status", "load_status",
            "status", "error_message",
        }
        actual = set(self._columns("ingestion_audit").keys())
        missing = expected - actual
        self.assertFalse(missing, f"ingestion_audit missing columns: {missing}")


class TestRawColumnTypes(RawSchemaTestBase):
    def test_financial_fields_use_numeric_not_float(self):
        numeric_fields = {
            "products": ["price", "rating"],
            "orders": ["total_amount"],
            "order_items": ["item_price", "item_total"],
        }
        for table, cols in numeric_fields.items():
            columns = self._columns(table)
            for col in cols:
                with self.subTest(table=table, column=col):
                    self.assertEqual(columns[col], "numeric")

    def test_date_and_timestamp_types(self):
        self.assertEqual(self._columns("users")["signup_date"], "date")
        self.assertEqual(self._columns("orders")["order_date"], "timestamp without time zone")
        self.assertEqual(
            self._columns("reviews")["review_date"], "timestamp without time zone"
        )
        self.assertEqual(
            self._columns("events")["event_timestamp"], "timestamp without time zone"
        )

    def test_quantity_and_rating_are_integer(self):
        self.assertEqual(self._columns("order_items")["quantity"], "integer")
        self.assertEqual(self._columns("reviews")["rating"], "integer")

    def test_ingestion_timestamp_metadata_is_timestamptz(self):
        for table in RAW_TABLES:
            with self.subTest(table=table):
                self.assertEqual(
                    self._columns(table)["ingestion_timestamp"],
                    "timestamp with time zone",
                )


class TestNoPkFkOnRawTables(RawSchemaTestBase):
    def _constraint_types(self, table_name: str) -> set[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT tc.constraint_type
                FROM information_schema.table_constraints tc
                WHERE tc.table_schema = 'raw' AND tc.table_name = %s
                """,
                (table_name,),
            )
            return {row[0] for row in cur.fetchall()}

    def test_no_primary_key_constraints_on_six_raw_tables(self):
        for table in RAW_TABLES:
            with self.subTest(table=table):
                self.assertNotIn("PRIMARY KEY", self._constraint_types(table))

    def test_no_foreign_key_constraints_on_six_raw_tables(self):
        for table in RAW_TABLES:
            with self.subTest(table=table):
                self.assertNotIn("FOREIGN KEY", self._constraint_types(table))

    def test_ingestion_audit_is_allowed_a_primary_key(self):
        # ingestion_audit is operational metadata, not a source mirror —
        # DEC-002's fidelity rationale does not apply to it (see
        # docs/architecture/raw_schema.md).
        self.assertIn("PRIMARY KEY", self._constraint_types("ingestion_audit"))


class TestDatabaseConnectionAndInitialization(RawSchemaTestBase):
    def test_database_connection_works(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)

    def test_initialization_is_repeatable_idempotent(self):
        # Rerunning init must not raise and must not change table counts.
        def _table_count():
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'raw'"
                )
                return cur.fetchone()[0]

        before = _table_count()
        init_raw_schema()
        init_raw_schema()
        after = _table_count()
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
