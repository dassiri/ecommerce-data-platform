-- =============================================================================
-- Step 3.3.2 — PostgreSQL Raw Schema & Ingestion Foundation
-- =============================================================================
-- Purpose:
--   Create the `raw` (Landing) schema: one table per source dataset, plus the
--   `raw.ingestion_audit` operational log table.
--
-- Scope / non-goals (see docs/architecture/raw_schema.md for full rationale):
--   - No PK or FK constraints on the six dataset tables (DEC-002). Raw must
--     preserve source data faithfully, including duplicate natural keys and
--     orphaned/questionable rows. Business/referential rules belong to later
--     layers (Staging/Curated).
--   - Nullability mirrors each column's `nullable` flag in its YAML data
--     contract (ingestion/contracts/*_contract.yml) — this is a structural
--     fact carried over from the contracts, not an invented business rule.
--   - No CHECK constraints for accepted_values / regex patterns / min-max
--     ranges. Those are deferred business/data-quality rules, not schema
--     constraints, per the approved contracts.
--   - This script does NOT load any data. It only creates structure.
--
-- Idempotency:
--   Every statement uses IF NOT EXISTS so this script is safe to rerun
--   against an already-initialized database (see database/init_db.py).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- -----------------------------------------------------------------------------
-- raw.users
-- Source: users.csv | Contract: ingestion/contracts/users_contract.yml
-- Grain: one row per user_id (source may contain duplicate natural keys;
--        not deduplicated or constrained here).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.users (
    user_id              TEXT            NOT NULL,
    name                 TEXT            NOT NULL,
    email                TEXT            NOT NULL,
    gender               TEXT            NOT NULL,
    city                 TEXT            NOT NULL,
    signup_date          DATE            NOT NULL,

    -- Ingestion metadata (DEC-004) — additive, not part of source data.
    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.products
-- Source: products.csv | Contract: ingestion/contracts/products_contract.yml
-- Grain: one row per product_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.products (
    product_id           TEXT            NOT NULL,
    product_name         TEXT            NOT NULL,
    category             TEXT            NOT NULL,
    brand                TEXT            NOT NULL,
    price                NUMERIC(12, 2)  NOT NULL,
    rating               NUMERIC(3, 2)   NOT NULL,

    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.orders
-- Source: orders.csv | Contract: ingestion/contracts/orders_contract.yml
-- Grain: one row per order_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id             TEXT            NOT NULL,
    user_id              TEXT            NOT NULL,
    order_date           TIMESTAMP       NOT NULL,
    order_status         TEXT            NOT NULL,
    total_amount         NUMERIC(12, 2)  NOT NULL,

    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.order_items
-- Source: order_items.csv | Contract: ingestion/contracts/order_items_contract.yml
-- Grain: one row per order_item_id (one product line within an order).
-- Note: order_items.user_id is redundant with orders.user_id (Step 1 finding);
--       preserved as-is, not removed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id        TEXT            NOT NULL,
    order_id             TEXT            NOT NULL,
    product_id           TEXT            NOT NULL,
    user_id              TEXT            NOT NULL,
    quantity             INTEGER         NOT NULL,
    item_price           NUMERIC(12, 2)  NOT NULL,
    item_total           NUMERIC(12, 2)  NOT NULL,

    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.reviews
-- Source: reviews.csv | Contract: ingestion/contracts/reviews_contract.yml
-- Grain: one row per review_id.
-- Note: reviews.user_id is redundant with orders.user_id (Step 1 finding);
--       preserved as-is. Reviews on non-completed orders are a known DQ
--       finding, not enforced here.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.reviews (
    review_id            TEXT            NOT NULL,
    order_id             TEXT            NOT NULL,
    product_id           TEXT            NOT NULL,
    user_id              TEXT            NOT NULL,
    rating               INTEGER         NOT NULL,
    review_text          TEXT            NOT NULL,
    review_date          TIMESTAMP       NOT NULL,

    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.events
-- Source: events.csv | Contract: ingestion/contracts/events_contract.yml
-- Grain: one row per event_id. No order linkage in source (Step 1 finding).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.events (
    event_id             TEXT            NOT NULL,
    user_id              TEXT            NOT NULL,
    product_id           TEXT            NOT NULL,
    event_type           TEXT            NOT NULL,
    event_timestamp      TIMESTAMP       NOT NULL,

    ingestion_timestamp  TIMESTAMPTZ     NOT NULL,
    source_file          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    ingestion_run_id     TEXT            NOT NULL
);

-- -----------------------------------------------------------------------------
-- raw.ingestion_audit
-- Operational log of ingestion runs. This is NOT a mirror of a source
-- dataset, so (unlike the six tables above) a surrogate primary key is
-- appropriate here — it does not conflict with DEC-002, whose rationale is
-- specifically about preserving source-data fidelity.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingestion_audit (
    audit_id             BIGSERIAL       PRIMARY KEY,

    ingestion_run_id     TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    dataset_name         TEXT            NOT NULL,
    source_file          TEXT            NOT NULL,
    file_checksum        TEXT,

    started_at           TIMESTAMPTZ     NOT NULL,
    completed_at         TIMESTAMPTZ,
    duration_seconds     NUMERIC(10, 3),

    rows_read            INTEGER,
    rows_loaded          INTEGER,

    validation_status    TEXT,
    load_status          TEXT,
    status               TEXT            NOT NULL,
    error_message        TEXT
);
