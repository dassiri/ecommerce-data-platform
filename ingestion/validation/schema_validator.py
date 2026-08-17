"""Validate a source CSV file against its Data Contract.

Scope (Step 3.2.5): structural/schema validation only.
- File existence
- Required columns present
- Unexpected (undeclared) columns
- Basic data-type checks (best-effort cast per declared data_type)
- Nullable requirements
- Primary-key presence and uniqueness

This module does NOT:
- Load data into PostgreSQL
- Perform cross-dataset referential-integrity checks
- Perform business/data-quality checks (financial reconciliation, etc.)
Those are explicitly out of scope for this step (see contract
`deferred_business_rules` and `known_data_quality_findings`).
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from datetime import datetime

from ingestion.validation.contract_loader import ColumnContract, DataContract

_DATE_FORMATS = ("%Y-%m-%d",)
_TIMESTAMP_PREFIX_LEN = 26  # "YYYY-MM-DDTHH:MM:SS.ffffff"


@dataclass
class ValidationIssue:
    row_number: int | None  # None => file/schema-level issue, not row-level
    column: str | None
    issue_type: str
    message: str


@dataclass
class ValidationResult:
    dataset_name: str
    file_path: str
    is_valid: bool
    row_count: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)

    def summary(self) -> str:
        status = "PASS" if self.is_valid else "FAIL"
        return (
            f"[{status}] {self.dataset_name} ({self.file_path}): "
            f"{self.row_count} rows, {len(self.issues)} issue(s)"
        )


def _cast_ok(value: str, data_type: str) -> bool:
    """Best-effort check that `value` is plausibly of `data_type`."""
    if value == "":
        # Emptiness is a nullable concern, handled separately.
        return True
    try:
        if data_type == "integer":
            int(value)
        elif data_type in ("decimal", "float"):
            float(value)
        elif data_type == "date":
            datetime.strptime(value, "%Y-%m-%d")
        elif data_type == "timestamp":
            # Accept ISO8601 with or without microseconds.
            datetime.fromisoformat(value)
        elif data_type == "boolean":
            if value.lower() not in ("true", "false", "0", "1"):
                return False
        # 'string' accepts anything.
        return True
    except (ValueError, TypeError):
        return False


def validate_file(contract: DataContract, file_path: str) -> ValidationResult:
    result = ValidationResult(
        dataset_name=contract.dataset_name, file_path=file_path, is_valid=True
    )

    if not os.path.isfile(file_path):
        result.is_valid = False
        result.issues.append(
            ValidationIssue(
                row_number=None,
                column=None,
                issue_type="file_missing",
                message=f"Source file not found: {file_path}",
            )
        )
        return result

    with open(file_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []

        expected_columns = set(contract.column_names)
        actual_columns = set(header)

        missing_columns = expected_columns - actual_columns
        for col in sorted(missing_columns):
            result.is_valid = False
            result.issues.append(
                ValidationIssue(
                    row_number=None,
                    column=col,
                    issue_type="missing_required_column",
                    message=f"Required column '{col}' is absent from the file",
                )
            )

        unexpected_columns = actual_columns - expected_columns
        for col in sorted(unexpected_columns):
            result.is_valid = False
            result.issues.append(
                ValidationIssue(
                    row_number=None,
                    column=col,
                    issue_type="unexpected_column",
                    message=f"Column '{col}' is not declared in the contract",
                )
            )

        if missing_columns:
            # Can't meaningfully validate rows without the declared columns.
            return result

        pk_seen: set[tuple] = set()
        pk_cols = contract.primary_key
        row_count = 0

        for row_number, row in enumerate(reader, start=2):  # header = row 1
            row_count += 1
            for col in contract.columns:
                _validate_cell(result, row_number, col, row.get(col.name, ""))

            pk_values = tuple(row.get(c, "") for c in pk_cols)
            if any(v == "" for v in pk_values):
                result.is_valid = False
                result.issues.append(
                    ValidationIssue(
                        row_number=row_number,
                        column=",".join(pk_cols),
                        issue_type="primary_key_null",
                        message="Primary key column(s) contain a null/empty value",
                    )
                )
            elif pk_values in pk_seen:
                result.is_valid = False
                result.issues.append(
                    ValidationIssue(
                        row_number=row_number,
                        column=",".join(pk_cols),
                        issue_type="primary_key_duplicate",
                        message=f"Duplicate primary key value: {pk_values}",
                    )
                )
            else:
                pk_seen.add(pk_values)

        result.row_count = row_count

        if row_count == 0:
            result.is_valid = False
            result.issues.append(
                ValidationIssue(
                    row_number=None,
                    column=None,
                    issue_type="empty_file",
                    message="File contains a header but zero data rows",
                )
            )

    return result


def _validate_cell(
    result: ValidationResult, row_number: int, col: ColumnContract, value: str
) -> None:
    if value == "" or value is None:
        if not col.nullable:
            result.is_valid = False
            result.issues.append(
                ValidationIssue(
                    row_number=row_number,
                    column=col.name,
                    issue_type="null_violation",
                    message=f"Column '{col.name}' is declared non-nullable but "
                    "value is empty",
                )
            )
        return

    if not _cast_ok(value, col.data_type):
        result.is_valid = False
        result.issues.append(
            ValidationIssue(
                row_number=row_number,
                column=col.name,
                issue_type="type_violation",
                message=f"Value '{value}' is not a valid {col.data_type}",
            )
        )
        return

    if col.accepted_values and value not in [str(v) for v in col.accepted_values]:
        result.is_valid = False
        result.issues.append(
            ValidationIssue(
                row_number=row_number,
                column=col.name,
                issue_type="accepted_value_violation",
                message=f"Value '{value}' not in accepted_values "
                f"{col.accepted_values}",
            )
        )


def validate_all(
    contracts: dict[str, DataContract], data_dir: str
) -> dict[str, ValidationResult]:
    """Validate every contract's source file located under `data_dir`."""
    results: dict[str, ValidationResult] = {}
    for name, contract in contracts.items():
        file_path = os.path.join(data_dir, contract.source_file)
        results[name] = validate_file(contract, file_path)
    return results
