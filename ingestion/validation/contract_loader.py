"""Load and lightly validate data-contract YAML files.

This module is intentionally narrow in scope (Step 3.2.5): it loads a
contract YAML file into a plain Python dict-based model and checks that the
contract itself is internally well-formed. It does NOT read or validate any
source CSV data — that is the job of ``schema_validator.py``.
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field

import yaml

REQUIRED_TOP_LEVEL_KEYS = (
    "dataset_name",
    "source_file",
    "grain",
    "primary_key",
    "columns",
)

REQUIRED_COLUMN_KEYS = ("name", "data_type", "nullable")

VALID_DATA_TYPES = frozenset(
    {"string", "integer", "decimal", "float", "date", "timestamp", "boolean"}
)


class ContractError(ValueError):
    """Raised when a contract file is missing, malformed, or inconsistent."""


@dataclass
class ColumnContract:
    name: str
    data_type: str
    nullable: bool
    constraints: list = field(default_factory=list)
    accepted_values: list | None = None
    source_notes: str | None = None


@dataclass
class DataContract:
    dataset_name: str
    source_file: str
    grain: str
    primary_key: list[str]
    columns: list[ColumnContract]
    description: str | None = None
    foreign_keys: list[dict] = field(default_factory=list)
    known_data_quality_findings: list[dict] = field(default_factory=list)
    deferred_business_rules: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> ColumnContract | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


def _validate_raw_structure(raw: dict, path: str) -> None:
    if not isinstance(raw, dict):
        raise ContractError(f"{path}: contract root must be a mapping/object")

    missing = [k for k in REQUIRED_TOP_LEVEL_KEYS if k not in raw]
    if missing:
        raise ContractError(f"{path}: missing required top-level key(s): {missing}")

    if not isinstance(raw["columns"], list) or not raw["columns"]:
        raise ContractError(f"{path}: 'columns' must be a non-empty list")

    seen_columns = set()
    for i, col in enumerate(raw["columns"]):
        if not isinstance(col, dict):
            raise ContractError(f"{path}: columns[{i}] must be a mapping/object")
        missing_col_keys = [k for k in REQUIRED_COLUMN_KEYS if k not in col]
        if missing_col_keys:
            raise ContractError(
                f"{path}: columns[{i}] missing required key(s): {missing_col_keys}"
            )
        if col["data_type"] not in VALID_DATA_TYPES:
            raise ContractError(
                f"{path}: columns[{i}] ('{col['name']}') has unsupported "
                f"data_type '{col['data_type']}'. Expected one of "
                f"{sorted(VALID_DATA_TYPES)}"
            )
        if not isinstance(col["nullable"], bool):
            raise ContractError(
                f"{path}: columns[{i}] ('{col['name']}') 'nullable' must be boolean"
            )
        if col["name"] in seen_columns:
            raise ContractError(
                f"{path}: duplicate column name '{col['name']}' in contract"
            )
        seen_columns.add(col["name"])

    if not isinstance(raw["primary_key"], list) or not raw["primary_key"]:
        raise ContractError(f"{path}: 'primary_key' must be a non-empty list")

    for pk_col in raw["primary_key"]:
        if pk_col not in seen_columns:
            raise ContractError(
                f"{path}: primary_key column '{pk_col}' is not defined in 'columns'"
            )

    for fk in raw.get("foreign_keys", []) or []:
        for req in ("column", "references_dataset", "references_column"):
            if req not in fk:
                raise ContractError(
                    f"{path}: foreign_keys entry missing required key '{req}'"
                )
        if fk["column"] not in seen_columns:
            raise ContractError(
                f"{path}: foreign key column '{fk['column']}' is not defined "
                "in 'columns'"
            )


def load_contract(path: str) -> DataContract:
    """Load a single contract YAML file and return a DataContract.

    Raises ContractError if the file is missing or malformed.
    """
    if not os.path.isfile(path):
        raise ContractError(f"Contract file not found: {path}")

    with open(path, "r", encoding="utf-8") as fh:
        try:
            raw = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ContractError(f"{path}: invalid YAML syntax: {exc}") from exc

    _validate_raw_structure(raw, path)

    columns = [
        ColumnContract(
            name=c["name"],
            data_type=c["data_type"],
            nullable=c["nullable"],
            constraints=c.get("constraints", []) or [],
            accepted_values=c.get("accepted_values"),
            source_notes=c.get("source_notes"),
        )
        for c in raw["columns"]
    ]

    return DataContract(
        dataset_name=raw["dataset_name"],
        source_file=raw["source_file"],
        grain=raw["grain"],
        primary_key=list(raw["primary_key"]),
        columns=columns,
        description=raw.get("description"),
        foreign_keys=raw.get("foreign_keys", []) or [],
        known_data_quality_findings=raw.get("known_data_quality_findings", []) or [],
        deferred_business_rules=raw.get("deferred_business_rules", []) or [],
        raw=raw,
    )


def load_all_contracts(contracts_dir: str) -> dict[str, DataContract]:
    """Load every ``*_contract.yml`` file in a directory.

    Returns a dict keyed by dataset_name.
    """
    contracts: dict[str, DataContract] = {}
    pattern = os.path.join(contracts_dir, "*_contract.yml")
    for path in sorted(glob.glob(pattern)):
        contract = load_contract(path)
        if contract.dataset_name in contracts:
            raise ContractError(
                f"Duplicate dataset_name '{contract.dataset_name}' found in "
                f"{path} and another contract file"
            )
        contracts[contract.dataset_name] = contract
    return contracts
