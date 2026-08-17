import os
import unittest

from ingestion.validation.contract_loader import load_contract
from ingestion.validation.schema_validator import validate_all, validate_file

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRACTS_DIR = os.path.join(PROJECT_ROOT, "ingestion", "contracts")
DATA_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw_source")
FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

USERS_CONTRACT_PATH = os.path.join(CONTRACTS_DIR, "users_contract.yml")


class TestSchemaValidatorValidInput(unittest.TestCase):
    def setUp(self):
        self.contract = load_contract(USERS_CONTRACT_PATH)

    def test_valid_fixture_passes(self):
        result = validate_file(
            self.contract, os.path.join(FIXTURES_DIR, "users_valid.csv")
        )
        self.assertTrue(result.is_valid, msg=result.issues)
        self.assertEqual(result.row_count, 3)
        self.assertEqual(result.issues, [])


class TestSchemaValidatorMalformedInput(unittest.TestCase):
    def setUp(self):
        self.contract = load_contract(USERS_CONTRACT_PATH)

    def test_file_missing_is_rejected(self):
        result = validate_file(self.contract, "/tmp/does_not_exist_12345.csv")
        self.assertFalse(result.is_valid)
        self.assertEqual(result.issues[0].issue_type, "file_missing")

    def test_missing_required_column_is_rejected(self):
        result = validate_file(
            self.contract, os.path.join(FIXTURES_DIR, "users_missing_column.csv")
        )
        self.assertFalse(result.is_valid)
        issue_types = {i.issue_type for i in result.issues}
        self.assertIn("missing_required_column", issue_types)

    def test_unexpected_column_is_rejected(self):
        result = validate_file(
            self.contract, os.path.join(FIXTURES_DIR, "users_extra_column.csv")
        )
        self.assertFalse(result.is_valid)
        issue_types = {i.issue_type for i in result.issues}
        self.assertIn("unexpected_column", issue_types)

    def test_duplicate_primary_key_is_rejected(self):
        result = validate_file(
            self.contract, os.path.join(FIXTURES_DIR, "users_duplicate_pk.csv")
        )
        self.assertFalse(result.is_valid)
        issue_types = {i.issue_type for i in result.issues}
        self.assertIn("primary_key_duplicate", issue_types)

    def test_null_and_invalid_values_are_rejected(self):
        result = validate_file(
            self.contract, os.path.join(FIXTURES_DIR, "users_invalid_values.csv")
        )
        self.assertFalse(result.is_valid)
        issue_types = {i.issue_type for i in result.issues}
        # empty `name` (non-nullable), invalid `gender` accepted_value,
        # invalid `signup_date` date format
        self.assertIn("null_violation", issue_types)
        self.assertIn("accepted_value_violation", issue_types)
        self.assertIn("type_violation", issue_types)


class TestSchemaValidatorAgainstRealSourceFiles(unittest.TestCase):
    """
    Validates the six real source CSVs against their registered contracts.
    Read-only: this test does not modify the source files.
    """

    def test_all_six_real_source_files_pass_schema_validation(self):
        contracts = {}
        for fname in (
            "users",
            "products",
            "orders",
            "order_items",
            "reviews",
            "events",
        ):
            path = os.path.join(CONTRACTS_DIR, f"{fname}_contract.yml")
            contract = load_contract(path)
            contracts[contract.dataset_name] = contract

        results = validate_all(contracts, DATA_RAW_DIR)
        for name, result in results.items():
            with self.subTest(dataset=name):
                self.assertTrue(result.is_valid, msg=result.issues[:5])
                self.assertGreater(result.row_count, 0)


if __name__ == "__main__":
    unittest.main()
