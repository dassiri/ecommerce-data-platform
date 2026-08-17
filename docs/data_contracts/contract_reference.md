# Data Contracts — Reference

Step: **3.2.5 — Source Data Registration & Data Contracts**
Scope: contract design and schema validation only. No PostgreSQL loading,
no ingestion pipeline, no transformations.

## 1. Purpose

Data contracts are machine-readable descriptions of the six raw source
CSV files. They give the (future) ingestion layer a single, version-
controlled definition of what a "valid" source file looks like structurally,
before any row is loaded into PostgreSQL Raw/Landing. They exist so that:

- Structural drift in a source file (missing/renamed/extra columns, wrong
  types) is caught at the door, not discovered downstream.
- The physical schema is documented once and reused by both the validator
  and by anyone onboarding onto the project.
- Business-quality concerns are kept clearly separate from structural
  concerns, so the contract layer stays stable even as business rules
  evolve.

## 2. Contract structure

Each contract is a YAML file at `ingestion/contracts/<dataset>_contract.yml`
with a consistent shape:

| Field | Meaning |
|---|---|
| `dataset_name` | Logical name of the dataset |
| `source_file` | Exact source CSV filename |
| `description` | Free-text summary |
| `grain` | What one row represents |
| `primary_key` | Column(s) that uniquely identify a row |
| `foreign_keys` | Columns referencing another dataset's primary key |
| `columns` | List of column definitions (see below) |
| `known_data_quality_findings` | Observations from profiling, carried forward from Step 1, documented but **not** enforced as schema rules |
| `deferred_business_rules` | Cross-field / cross-dataset rules explicitly pushed to a later Data Quality layer |

Each entry in `columns` has:

| Field | Meaning |
|---|---|
| `name` | Column name (matches the CSV header exactly) |
| `data_type` | One of `string`, `integer`, `decimal`, `date`, `timestamp`, `boolean` |
| `nullable` | Whether an empty value is acceptable |
| `constraints` | Structural constraints (uniqueness, pattern, min/max, accepted values) |
| `accepted_values` | Enumerated legal values, where the source clearly supports one |
| `source_notes` | Profiling observations backing the above |

## 3. The six registered datasets

| Dataset | Source file | Grain | Primary key | Rows profiled |
|---|---|---|---|---|
| users | `users.csv` | one row per user | `user_id` | 10,000 |
| products | `products.csv` | one row per product | `product_id` | 2,000 |
| orders | `orders.csv` | one row per order | `order_id` | 20,000 |
| order_items | `order_items.csv` | one row per order line item | `order_item_id` | 43,525 |
| reviews | `reviews.csv` | one row per review | `review_id` | 15,000 |
| events | `events.csv` | one row per interaction event | `event_id` | 80,000 |

Foreign keys:

- `orders.user_id` → `users.user_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `order_items.user_id` → `users.user_id` (redundant with the parent order's
  `user_id` — see Step 1 finding below)
- `reviews.order_id` → `orders.order_id`
- `reviews.product_id` → `products.product_id`
- `reviews.user_id` → `users.user_id` (redundant, same reasoning)
- `events.user_id` → `users.user_id`
- `events.product_id` → `products.product_id`

All of the above foreign-key relationships were re-verified directly
against the current CSV files: **0 orphaned foreign-key values** were
found in the profiled extracts across all six datasets.

## 4. What is enforced now (schema layer)

The validator (`ingestion/validation/schema_validator.py`) checks, per file:

- The file exists.
- Every column declared in the contract is present in the CSV header.
- No undeclared/unexpected columns are present.
- Every value is castable to its declared `data_type`.
- Non-nullable columns contain no empty values.
- `accepted_values` (where declared) are respected.
- The declared primary key is present (non-null) and unique across rows.

This is deliberately narrow: **structural correctness only**, per Task 3 /
Task 6 of this step.

## 5. What is intentionally deferred to Data Quality

The following are documented on the relevant contracts under
`known_data_quality_findings` / `deferred_business_rules`, but are **not**
enforced by the schema validator, because they are business/data-quality
rules rather than structural rules:

- **Financial reconciliation** — `order_items.item_total == quantity *
  item_price`, and `sum(order_items.item_total) per order == orders.total_amount`.
  Both were spot-checked during profiling and held for 100% of rows/orders
  in the current extract, but this is a cross-field / cross-dataset
  arithmetic rule, not a schema rule, and belongs in the Data Quality layer
  going forward.
- **Reviews on non-completed orders** — the majority of `reviews.csv` rows
  reference orders whose `order_status` is not `completed`. This confirms
  the Step 1 finding. Per Task 3's explicit instruction, "every review
  must belong to a completed order" is **not** made a schema constraint,
  since no source or business requirement establishes it as a hard rule.
- **Duplicate natural-key observations** — `order_items` has 29 rows
  sharing an `(order_id, product_id)` pair; `reviews` has 6 rows sharing
  `(order_id, product_id, user_id)` and 12 sharing `(user_id, product_id)`.
  These may reflect legitimate business scenarios (e.g., a product added to
  an order twice) rather than errors, so they are documented as
  observations, not uniqueness constraints.
- **`products.rating` vs. aggregated `reviews.rating` reconciliation** —
  whether the static product-level rating should match an aggregate of its
  reviews is a business question, deferred.
- **`review_text` low cardinality anomaly** — only 10 distinct values
  across 15,000 `reviews.csv` rows, suggesting synthetic/templated text.
  Flagged for awareness; not corrected here (Task 4 — Preserve Source Truth).
- **Purchase-event-to-order reconciliation** — `events.csv` has no
  `order_id`/`order_item_id` column, so `purchase` events cannot be
  directly joined to `orders`/`order_items`. Any volume-based
  reconciliation is approximate and deferred.

## 6. Relationship to Step 1 findings

The task brief for this step lists five known findings from the approved
Step 1 assessment. Each was re-verified directly against the current CSV
files during this step's profiling, and is carried into the contracts as
follows:

| Step 1 finding | Where it lives in the contracts | Re-verified? |
|---|---|---|
| Duplicate natural-key observations | `known_data_quality_findings` on `order_items` and `reviews` contracts | Yes — 29 / 6+12 rows respectively |
| Missing `updated_at` / change-tracking limitation | `known_data_quality_findings` on `users` and `orders` contracts | Yes — no such column exists on any of the six files |
| Reviews associated with cancelled/processing orders | `known_data_quality_findings` on `reviews` contract, explicitly kept out of the schema constraints per Task 3 | Yes — ~80% of reviews reference non-completed orders |
| `user_id` redundancy across related datasets | Documented on the `user_id` foreign key of `order_items` and `reviews` contracts | Yes — `order_items.user_id` and `reviews.user_id` match their parent order's `user_id` for 100% of rows |
| Financial validation requirements | `deferred_business_rules` on `orders` and `order_items` contracts | Yes — 0 mismatches found in the profiled extract |

**Note on provenance:** this working session's environment did not have a
persisted artifact from the earlier approved Step 1 write-up available on
disk to diff against byte-for-byte. In place of a document diff, the five
findings named explicitly in this step's task brief were independently
re-derived by profiling the actual CSV files fresh (row counts, null
rates, key uniqueness, referential integrity, and the specific patterns
named above). All five were confirmed present in the current data with no
contradictions. If a saved Step 1 document differs from what's summarized
here in ways not covered by these five points, please flag it and it will
be reconciled explicitly rather than silently overridden.

## 7. Relationship between contracts and ingestion

The contracts are the input to the (not-yet-built) ingestion loader. The
intended flow for a future step is:

1. Ingestion loader reads a source CSV.
2. Loader calls `schema_validator.validate_file()` (or `validate_all()`)
   against the matching contract.
3. If validation fails, the file is rejected before touching PostgreSQL
   Raw/Landing, and issues are surfaced for triage.
4. If validation passes, the (still out-of-scope-for-this-step) loader
   proceeds to load the raw, untransformed rows into PostgreSQL.

No loader, DAG, or PostgreSQL write path is implemented in this step.

## 8. Discrepancies between Step 1 and the actual CSV files

None found. Every finding named in the task brief (duplicates, missing
`updated_at`, reviews on non-completed orders, `user_id` redundancy,
financial reconciliation) was reproduced against the live CSV files during
this step's profiling. No contradictions were observed, so nothing was
silently corrected and no STOP condition was triggered.

## 9. Tooling note

`black`, `ruff`, `mypy`, and `pytest` are not installed in this execution
environment and outbound network access is disabled, so they could not be
installed or run here. As a substitute:

- Syntax/compile correctness was verified with `python -m py_compile` on
  every new module.
- The test suite (`tests/test_contract_loader.py`,
  `tests/test_schema_validator.py`) was written against and run with the
  Python standard library's `unittest` runner, which requires no
  installation.
- `pyproject.toml` includes `[tool.black]`, `[tool.ruff]`, and
  `[tool.mypy]` configuration so these tools run correctly once available
  in a networked CI environment.

Running them there is recommended before merging:

```
black --check .
ruff check .
mypy ingestion
pytest -v
```
