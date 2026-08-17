import os
import unittest

from ingestion.validation.contract_loader import (
    ContractError,
    load_all_contracts,
    load_contract,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRACTS_DIR = os.path.join(PROJECT_ROOT, "ingestion", "contracts")
FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

EXPECTED_DATASETS = {
    "users",
    "products",
    "orders",
    "order_items",
    "reviews",
    "events",
}


class TestContractLoader(unittest.TestCase):
    def test_all_six_contracts_load_successfully(self):
        contracts = load_all_contracts(CONTRACTS_DIR)
        self.assertEqual(set(contracts.keys()), EXPECTED_DATASETS)

    def test_each_contract_has_required_top_level_fields(self):
        contracts = load_all_contracts(CONTRACTS_DIR)
        for name, contract in contracts.items():
            with self.subTest(dataset=name):
                self.assertTrue(contract.dataset_name)
                self.assertTrue(contract.source_file)
                self.assertTrue(contract.grain)
                self.assertTrue(contract.primary_key)
                self.assertTrue(contract.columns)

    def test_primary_key_columns_are_declared_columns(self):
        contracts = load_all_contracts(CONTRACTS_DIR)
        for name, contract in contracts.items():
            with self.subTest(dataset=name):
                for pk_col in contract.primary_key:
                    self.assertIn(pk_col, contract.column_names)

    def test_foreign_keys_reference_declared_columns(self):
        contracts = load_all_contracts(CONTRACTS_DIR)
        for name, contract in contracts.items():
            with self.subTest(dataset=name):
                for fk in contract.foreign_keys:
                    self.assertIn(fk["column"], contract.column_names)

    def test_known_business_rules_are_documented_not_schema_enforced(self):
        # Sanity check that Step 1 known findings appear as documentation
        # (known_data_quality_findings / deferred_business_rules) rather
        # than as hard schema constraints.
        contracts = load_all_contracts(CONTRACTS_DIR)
        reviews = contracts["reviews"]
        finding_ids = {f["id"] for f in reviews.known_data_quality_findings}
        self.assertIn("reviews_on_non_completed_orders", finding_ids)

        order_items = contracts["order_items"]
        oi_finding_ids = {f["id"] for f in order_items.known_data_quality_findings}
        self.assertIn("duplicate_order_product_natural_key", oi_finding_ids)

    def test_missing_file_raises_contract_error(self):
        with self.assertRaises(ContractError):
            load_contract(os.path.join(CONTRACTS_DIR, "does_not_exist_contract.yml"))

    def test_malformed_yaml_raises_contract_error(self):
        bad_path = os.path.join(FIXTURES_DIR, "_malformed_contract.yml")
        with open(bad_path, "w", encoding="utf-8") as fh:
            fh.write("dataset_name: broken\ncolumns: [this is not: valid: yaml:")
        try:
            with self.assertRaises(ContractError):
                load_contract(bad_path)
        finally:
            os.remove(bad_path)

    def test_contract_missing_required_key_raises_contract_error(self):
        bad_path = os.path.join(FIXTURES_DIR, "_incomplete_contract.yml")
        with open(bad_path, "w", encoding="utf-8") as fh:
            fh.write("dataset_name: incomplete\nsource_file: x.csv\n")
        try:
            with self.assertRaises(ContractError):
                load_contract(bad_path)
        finally:
            os.remove(bad_path)


if __name__ == "__main__":
    unittest.main()
