# E-Commerce Data Platform

## Project Overview

This project is an end-to-end e-commerce analytics data platform. It ingests six source datasets into PostgreSQL, transforms them through a layered dbt warehouse (Staging, Curated, Marts, Consumption), and enforces data quality with dbt generic tests on the Curated layer.

The implementation includes a Python ingestion pipeline, YAML data contracts, schema validation, and SQL transformations designed for reproducible local development and portfolio demonstration.

## Business Problem

An e-commerce business needs a reliable, queryable view of customers, products, orders, and behavior to answer questions such as:

- How much revenue and volume are we generating?
- Who are our highest-value customers?
- Which products drive sales?

This platform consolidates disparate CSV sources into a structured warehouse, applies consistent grains and relationships, and produces analytics-ready outputs for reporting and customer segmentation.

## Architecture

The warehouse follows a five-layer pattern:

```text
CSV Sources
    ↓
Python Ingestion
    ↓
Raw (PostgreSQL)
    ↓
Staging (dbt views)
    ↓
Curated (dbt tables)
    ↓
Marts (dbt tables)
    ↓
Consumption (dbt tables)
```

| Layer | Schema (PostgreSQL) | Materialization | Purpose |
|---|---|---|---|
| Raw | `raw` | Tables (Python-loaded) | Landing zone; structurally typed copy of source CSVs plus ingestion metadata |
| Staging | `staging` | Views | Light interface to Raw sources via dbt `source()` |
| Curated | `curated` | Tables | Conformed dimensions and facts at entity grain |
| Marts | `marts` | Tables | Subject-area analytics models (sales by line, customer, product) |
| Consumption | `consumption` | Tables | Business-facing KPI and segmentation outputs |

Detailed Raw-layer design is documented in `docs/architecture/raw_schema.md`.

## Technology Stack

| Technology | Role |
|---|---|
| Python | Database utilities, CSV ingestion, contract validation, unit tests |
| PostgreSQL | Warehouse database (`ecommerce_dw`) |
| dbt | Staging, Curated, Marts, and Consumption transformations and tests |
| SQL | Transformations across all dbt layers |

## Data Pipeline

### Raw

- Six tables mirror the source CSVs: `users`, `products`, `orders`, `order_items`, `reviews`, `events`.
- Ingestion metadata columns (`ingestion_timestamp`, `source_file`, `batch_id`, `ingestion_run_id`) are appended per approved design (DEC-004).
- Raw preserves source fidelity: no deduplication, cleaning, or business-rule enforcement at load time (DEC-002).
- Loaded by `python -m ingestion.raw_loader` after schema initialization.

### Staging

- Six views (`stg_*`) select business columns from Raw sources.
- Provides a stable dbt interface (`source('raw', ...)`) for downstream models.
- Materialized as views in the `staging` schema.

### Curated

- Three dimension models (`dim_users`, `dim_products`, `dim_orders`) and three fact models (`fct_order_items`, `fct_reviews`, `fct_events`).
- Entity-grain conformed tables used as the semantic foundation for analytics.
- Enforced with 30 dbt generic tests (primary-key uniqueness, not-null, and referential integrity).

### Marts

- `mart_sales` — line-item grain; denormalized sales detail.
- `mart_customer_sales` — customer grain; aggregated purchase metrics.
- `mart_product_sales` — product grain; aggregated sales performance.

All marts use **gross, all-status** sales (no filter on `order_status`); cancelled, returned, processing, shipped, and completed orders are included consistently.

### Consumption

- `sales_summary` — single-row platform KPI snapshot (orders, items, quantity, revenue, AOV).
- `customer_segmentation` — customer-level tiers (VIP, Loyal, Regular, One-Time) derived from purchase behavior.

## Data Sources

Six registered datasets, each with a YAML contract in `ingestion/contracts/`:

| Dataset | Source file | Grain | Primary key | Profiled rows |
|---|---|---|---|---|
| users | `users.csv` | One row per user | `user_id` | 10,000 |
| products | `products.csv` | One row per product | `product_id` | 2,000 |
| orders | `orders.csv` | One row per order | `order_id` | 20,000 |
| order_items | `order_items.csv` | One row per line item | `order_item_id` | 43,525 |
| reviews | `reviews.csv` | One row per review | `review_id` | 15,000 |
| events | `events.csv` | One row per event | `event_id` | 80,000 |

Key relationships: orders reference users; order items reference orders, products, and users; reviews reference orders, products, and users; events reference users and products. Events have no direct link to orders.

## dbt Models

All production dbt models live under `warehouse/dbt/models/`.

### Staging (6 models — views)

| Model | Source table |
|---|---|
| `stg_users` | `raw.users` |
| `stg_products` | `raw.products` |
| `stg_orders` | `raw.orders` |
| `stg_order_items` | `raw.order_items` |
| `stg_reviews` | `raw.reviews` |
| `stg_events` | `raw.events` |

### Curated (6 models — tables)

| Model | Type | Grain |
|---|---|---|
| `dim_users` | Dimension | `user_id` |
| `dim_products` | Dimension | `product_id` |
| `dim_orders` | Dimension | `order_id` |
| `fct_order_items` | Fact | `order_item_id` |
| `fct_reviews` | Fact | `review_id` |
| `fct_events` | Fact | `event_id` |

### Marts (3 models — tables)

| Model | Grain | Upstream |
|---|---|---|
| `mart_sales` | `order_item_id` | `fct_order_items`, `dim_orders`, `dim_products`, `dim_users` |
| `mart_customer_sales` | `user_id` | `dim_users`, `dim_orders`, `fct_order_items` |
| `mart_product_sales` | `product_id` | `dim_products`, `fct_order_items`, `dim_orders` |

### Consumption (2 models — tables)

| Model | Grain | Upstream |
|---|---|---|
| `sales_summary` | Single-row aggregate | `mart_sales` |
| `customer_segmentation` | `user_id` | `mart_customer_sales` |

## Data Quality

Curated-layer tests are defined in `warehouse/dbt/models/curated/schema.yml`.

| Test type | Count | Purpose |
|---|---|---|
| `not_null` | 10 | Required keys and foreign keys must be populated |
| `unique` | 6 | Primary keys on all six curated models |
| `relationships` | 14 | Foreign-key integrity to parent dimensions |
| **Total** | **30** | |

### Test coverage by model

| Model | `not_null` | `unique` | `relationships` |
|---|---|---|---|
| `dim_users` | `user_id` | `user_id` | — |
| `dim_products` | `product_id` | `product_id` | — |
| `dim_orders` | `order_id`, `user_id` | `order_id` | `user_id` → `dim_users` |
| `fct_order_items` | PK + 3 FKs | `order_item_id` | → `dim_orders`, `dim_products`, `dim_users` |
| `fct_reviews` | PK + 3 FKs | `review_id` | → `dim_orders`, `dim_products`, `dim_users` |
| `fct_events` | PK + 2 FKs | `event_id` | → `dim_users`, `dim_products` |

Structural validation at ingestion time is handled separately by `ingestion/validation/schema_validator.py` against the YAML contracts. Cross-field business rules (for example, net sales excluding cancelled orders) are documented in contracts but deferred to a later data-quality layer.

## Validation Results

Validated against the built warehouse (`ecommerce_dw`):

| Check | Result |
|---|---|
| dbt build (models + tests) | **47/47 checks passed** |
| Order line items | **43,525** |
| Distinct orders | **20,000** |
| Purchasing customers (in marts) | **8,635** |
| Products (catalog) | **2,000** |
| Total revenue (`item_total` / `total_spend` / `total_revenue`) | **11,918,668.95** |
| Total quantity | **60,818** |
| Cross-mart reconciliation | **Passed** — revenue, quantity, order items, and distinct orders reconcile exactly across `mart_sales`, `mart_customer_sales`, and `mart_product_sales`; no INNER JOIN population loss on the line-item spine |

## Business Outputs

### `sales_summary`

Platform-wide KPIs in a single row:

- `total_orders`, `total_order_items`, `total_quantity`, `total_revenue`, `average_order_value`
- All-time gross snapshot aggregated from `mart_sales`

### `customer_segmentation`

Customer value tiers based on `mart_customer_sales` metrics:

| Segment | Rule |
|---|---|
| VIP | `total_spend >= 5000` |
| Loyal | `total_orders >= 5` and `total_spend < 5000` |
| Regular | `total_orders` between 2 and 4 |
| One-Time | `total_orders = 1` |

Only customers with at least one order appear (8,635 of 10,000 registered users).

## Key Design Decisions / Assumptions

| Decision | Description |
|---|---|
| Gross / all-status sales | Marts and Consumption include all `order_status` values (processing, completed, cancelled, returned, shipped). No net-sales filter is applied. |
| Raw fidelity (DEC-002) | No PK/FK constraints on Raw source tables; known duplicate natural keys and redundant columns are preserved at load. |
| Daily full batch (DEC-001) | Raw is a current snapshot via truncate-and-reload, not a historical CDC store. |
| Checksum idempotency (DEC-003) | Unchanged source files can be skipped (`SKIPPED_UNCHANGED`) based on SHA-256 checksum. |
| Referential integrity in Curated | Uniqueness and relationships are enforced with dbt tests, not database constraints. |
| INNER JOIN marts | Marts exclude orphan keys; validated with zero row loss on the line-item spine in the current dataset. |
| Deferred business rules | Examples documented but not enforced: reviews on non-completed orders, reconciliation of `products.rating` to review averages, `purchase` events to orders. |
| Reviews and events | Curated facts exist but are not yet used in Marts or Consumption models. |

See `docs/decision_log/DECISIONS.md` and `docs/architecture/raw_schema.md` for full rationale.

## Project Structure

```text
ecommerce-data-platform/
├── database/                  # PostgreSQL connection utilities and schema init
│   ├── connection.py
│   ├── init_db.py             # python -m database.init_db
│   └── schema/raw_schema.sql
├── docs/
│   ├── architecture/          # Raw schema and ingestion design
│   ├── data_contracts/        # Contract reference
│   └── decision_log/          # Approved architectural decisions
├── ingestion/
│   ├── contracts/             # YAML data contracts (6 datasets)
│   ├── validation/            # Contract loader and schema validator
│   ├── raw_loader.py          # python -m ingestion.raw_loader
│   ├── audit.py
│   └── checksum.py
├── tests/                     # Python unit and integration tests
├── warehouse/dbt/             # Primary dbt project (ecommerce_dw)
│   ├── dbt_project.yml
│   ├── profiles/
│   │   └── profiles.yml.example
│   ├── macros/
│   └── models/
│       ├── staging/           # 6 stg_* views
│       ├── curated/           # 6 dim/fct tables + schema.yml tests
│       ├── marts/             # 3 mart_* tables
│       └── consumption/       # 2 consumption tables
├── .env.example               # PostgreSQL connection template
├── requirements.txt           # Python runtime (psycopg2, PyYAML)
├── requirements-dbt.txt       # dbt-core, dbt-postgres
└── requirements-dev.txt       # Dev/test tooling
```

Note: `ecommerce_dbt/` is a separate dbt starter scaffold and is not the production warehouse project.

## How to Run

### Prerequisites

- PostgreSQL instance with database `ecommerce_dw`
- Python 3.14+ (dbt 1.12 requirement per `requirements-dbt.txt`)
- Dependencies installed:

```bash
pip install -r requirements.txt
pip install -r requirements-dbt.txt
```

### 1. Configure connections

Copy environment and dbt profile templates and fill in local credentials:

```bash
cp .env.example .env
cp warehouse/dbt/profiles/profiles.yml.example warehouse/dbt/profiles/profiles.yml
```

PostgreSQL settings are read from `.env` by the Python ingestion layer (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).

The dbt project (`warehouse/dbt/dbt_project.yml`) expects profile name `ecommerce_dbt`. Ensure your active dbt profile resolves to the same database before running dbt commands.

### 2. Initialize Raw schema

```bash
python -m database.init_db
```

### 3. Load source CSVs into Raw

Place source CSV files where the ingestion loader expects them (see `docs/architecture/ingestion_implementation.md`), then:

```bash
python -m ingestion.raw_loader
```

### 4. Run dbt (from `warehouse/dbt/`)

Commands documented in the project:

```bash
cd warehouse/dbt

dbt debug --profiles-dir profiles
dbt run --profiles-dir profiles
dbt test --profiles-dir profiles
```

To build all models and run tests in one step:

```bash
dbt build --profiles-dir profiles
```

### 5. Query Consumption outputs

After a successful build, analytics outputs are available in PostgreSQL:

- `consumption.sales_summary`
- `consumption.customer_segmentation`

## Future Enhancements

Realistic next steps supported by the current architecture:

- **Product-level consumption model** — KPI or ranking mart built on `mart_product_sales`
- **BI dashboard** — connect a visualization tool to Consumption tables
- **Additional data quality rules** — net-sales filters, cross-field arithmetic tests, review/event business rules documented in contracts
- **Use `fct_reviews` and `fct_events` in Marts** — satisfaction analytics and funnel/conversion models
- **Richer documentation** — dbt model descriptions, lineage docs (`dbt docs generate`), and consumption metric dictionaries
- **Orchestration** — schedule ingestion and dbt runs (not implemented today)
