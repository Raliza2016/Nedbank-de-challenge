# Nedbank Data Engineering Challenge — Medallion Pipeline

A modular medallion data pipeline (Bronze → Silver → Gold) built with **PySpark
3.5** and **Delta Lake 3.1** that ingests batch and streaming banking data into
a star-schema warehouse plus low-latency feature tables for downstream AI
scoring — all within a 2 GB / 2 vCPU container budget.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                   /data/input/                          │
                    │  accounts.csv   customers.csv   transactions.jsonl      │
                    └────────────────────────┬────────────────────────────────┘
                                             │
                                ┌────────────▼────────────┐
                                │  ingest.py  (BRONZE)    │
                                │  • explicit StructType  │
                                │  • PERMISSIVE +         │
                                │    _corrupt_record      │
                                │  • single ingestion_ts  │
                                └────────────┬────────────┘
                                             │  Delta
                                ┌────────────▼────────────┐
                                │  transform.py (SILVER)  │
                                │  • dedup on natural key │
                                │  • date parsing (3 fmts)│
                                │  • currency standardise │
                                │  • DQ flag priority     │
                                │    chain (4 levels)     │
                                └────────────┬────────────┘
                                             │  Delta
                                ┌────────────▼────────────┐
                                │  provision.py (GOLD)    │
                                │  • dim_customers (9 col)│
                                │  • dim_accounts (11 col)│  ← inner-joins to
                                │  • fact_transactions    │     dim_customers
                                │      (15 col)           │     (Q2 guarantee)
                                │  • OPTIMIZE + ZORDER    │
                                │  • dq_report.json       │
                                └────────────┬────────────┘
                                             │
                  ┌──────────────────────────┼──────────────────────────┐
                  │                          │                          │
              SQL Q1                      SQL Q2                     SQL Q3
       4 distinct txn_types        0 orphan accounts          1–9 province rows


   /data/stream/*.jsonl ─► stream_ingest.py (Stage 3) ─► stream_gold/
                            • signed-delta balance       current_balances/
                            • Delta MERGE upserts        recent_transactions/
                            • per-file error recovery
                            • 60 s quiesce, 20 s poll
```

---

## Repository Layout

```
pipeline-submission/
├── Dockerfile                    # Extends nedbank-de-challenge/base:1.0
├── requirements.txt              # Extra deps (none — base image is sufficient)
├── pytest.ini                    # Local test configuration
├── pipeline/
│   ├── __init__.py
│   ├── spark_session.py          # SparkSession factory + config loader
│   ├── logging_config.py         # JSON structured logger (banking-grade obs.)
│   ├── ingest.py                 # Bronze layer (explicit schemas)
│   ├── transform.py              # Silver layer (DQ flagging + dedup)
│   ├── provision.py              # Gold layer (dim/fact + OPTIMIZE/ZORDER)
│   ├── stream_ingest.py          # Stage 3 streaming with error recovery
│   └── run_all.py                # Pipeline entry point
├── config/
│   ├── pipeline_config.yaml      # Paths, stage, Spark settings
│   └── dq_rules.yaml             # Data quality rules
├── tests/                        # pytest suite (dev-only; not in image)
│   ├── conftest.py
│   ├── test_surrogate_keys.py
│   ├── test_currency_standardisation.py
│   ├── test_date_parsing.py
│   ├── test_age_band.py
│   ├── test_balance_arithmetic.py
│   ├── test_dq_flag_priority.py
│   └── test_orphan_exclusion.py
├── adr/
│   └── stage3_adr.md             # Architecture Decision Record
├── stream/
│   └── .gitkeep                  # Stream batch files dropped here at runtime
└── README.md
```

---

## Quick Start

### 1. Build the submission image

```bash
docker build -t my-submission:test .
```

### 2. Run with the scoring-harness command

```bash
docker run --rm \
  -v /path/to/data:/data \
  -m 4g --cpus="2" \
  my-submission:test
```

The `/path/to/data` directory must contain `input/` (with `accounts.csv`,
`customers.csv`, `transactions.jsonl`) and a writable `output/`. A `config/`
subdirectory is optional — if absent, the image-baked defaults at
`/app/config/` are used automatically. For Stage 3, also include a
`stream/` subdirectory with the micro-batch JSONL files.

### 3. Run the local test harness

```bash
bash ../run_tests.sh --stage 1 \
  --data-dir /tmp/test-data \
  --image my-submission:test
```

### 4. Run the unit test suite

```bash
pip install pytest
pytest        # ~30 tests, ~45 s on a laptop
```

### 5. Submit

```bash
git tag -a stage1-submission -m "Stage 1 submission"
git push origin stage1-submission
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Pipeline completed and all Q1 / Q2 / Q3 contracts verified |
| `1` | Pipeline failed — check the JSON log for the `pipeline.failed` event (it carries `error` and a full traceback in `exception`) |

The most common ways the pipeline exits `1`:

- **Missing input** — `bronze.accounts.failed` (or `transactions` / `customers`) with a `FileNotFoundError`. Re-check the `-v /path/to/data:/data` mount.
- **Q1/Q2/Q3 contract violation** — `gold.contracts.failed` with a `RuntimeError` that names the broken invariant and the measured value. The Gold tables and `dq_report.json` are still written, so you can inspect them.
- **Out-of-memory** — `silver.transactions.failed` is the usual offender at Stage 2 scale. Adjust `spark.driver_memory` / `spark.executor_memory` in `config/pipeline_config.yaml`.
- **Misconfigured `DQ_RULES_PATH`** — if you set it but the file is missing, the pipeline raises `FileNotFoundError` rather than silently falling back. Either unset it or fix the path.

### Reproducible reruns (`RUN_TIMESTAMP`)

Set `RUN_TIMESTAMP` to an ISO-8601 timestamp to pin the value the pipeline writes into every Bronze row's `ingestion_timestamp` column and the `run_timestamp` field in `dq_report.json`. Two reruns on the same input with the same `RUN_TIMESTAMP` produce byte-identical Parquet output:

```bash
docker run --rm -v /path/to/data:/data -m 4g --cpus="2" \
  -e RUN_TIMESTAMP=2026-04-27T12:00:00Z \
  my-submission:test
```

Without `RUN_TIMESTAMP`, the pipeline uses wall-clock UTC time (so reruns differ in those two columns even though every other column is deterministic via SHA-256 surrogate keys).

---

## Data Dictionary

### `dim_customers` — 9 fields

| Column | Type | Description |
|---|---|---|
| `customer_sk` | BIGINT | Surrogate key, deterministic SHA-256 hash of `customer_id` |
| `customer_id` | STRING | Natural key from source system |
| `gender` | STRING | F / M / OTHER |
| `province` | STRING | One of South Africa's 9 provinces (drives Q3) |
| `income_band` | STRING | LOW / MID / HIGH |
| `segment` | STRING | Mass / Affluent / Private |
| `risk_score` | INT | 0–1000 risk band |
| `kyc_status` | STRING | VERIFIED / PENDING / REJECTED |
| `age_band` | STRING | 18-25, 26-35, 36-45, 46-55, 56-65, 65+ |

### `dim_accounts` — 11 fields

| Column | Type | Description |
|---|---|---|
| `account_sk` | BIGINT | Surrogate key |
| `account_id` | STRING | Natural key |
| `customer_id` | STRING | Renamed from `customer_ref` (Silver) — Q2 join key |
| `account_type` | STRING | CHEQUE / SAVINGS / CREDIT |
| `account_status` | STRING | ACTIVE / DORMANT / CLOSED |
| `open_date` | DATE | Standardised ISO date |
| `product_tier` | STRING | STANDARD / PREMIUM / PRIVATE |
| `digital_channel` | STRING | ONLINE / MOBILE / BRANCH |
| `credit_limit` | DECIMAL(18,2) | |
| `current_balance` | DECIMAL(18,2) | |
| `last_activity_date` | DATE | |

### `fact_transactions` — 15 fields

| Column | Type | Description |
|---|---|---|
| `transaction_sk` | BIGINT | Surrogate key |
| `transaction_id` | STRING | Natural key |
| `account_sk` | BIGINT | FK → dim_accounts |
| `customer_sk` | BIGINT | FK → dim_customers |
| `transaction_date` | DATE | |
| `transaction_timestamp` | TIMESTAMP | Composed from date + time |
| `transaction_type` | STRING | CREDIT / DEBIT / REVERSAL / FEE (drives Q1) |
| `merchant_category` | STRING | |
| `merchant_subcategory` | STRING | NULL at Stage 1 |
| `amount` | DECIMAL(18,2) | |
| `currency` | STRING | Always "ZAR" after Silver normalisation |
| `channel` | STRING | |
| `province` | STRING | Sourced from `location.province` |
| `dq_flag` | STRING | NULL when clean; one of the four DQ codes otherwise |
| `ingestion_timestamp` | TIMESTAMP | Pipeline run timestamp |

### `stream_gold/current_balances` — 4 fields (Stage 3)

| Column | Type | Description |
|---|---|---|
| `account_id` | STRING | One row per account |
| `current_balance` | DECIMAL(18,2) | Running balance, accumulated via signed deltas |
| `last_transaction_timestamp` | TIMESTAMP | Newest event seen |
| `updated_at` | TIMESTAMP | When this row was last MERGEd |

### `stream_gold/recent_transactions` — 7 fields (Stage 3)

Last 50 transactions per account, retained for low-latency model scoring.

---

## Data Quality Rules

| Rule | Code | Action | Priority |
|---|---|---|---|
| Null primary key | `NULL_REQUIRED` | reject (filter out) | — |
| Orphaned account | `ORPHANED_ACCOUNT` | flag (kept in Silver, excluded from Gold) | 1 (highest) |
| Type mismatch (amount) | `TYPE_MISMATCH` | cast or flag | 2 |
| Date format variant | `DATE_FORMAT` | standardise or flag | 3 |
| Currency variant | `CURRENCY_VARIANT` | standardise (set ZAR, keep flag) | 4 (lowest) |
| Duplicate by natural key | `DUPLICATE_DEDUPED` | deduplicate (keep earliest) | — |

A record can have multiple issues; only the highest-priority flag is recorded
in `dq_flag`. This is a strict contract to prevent double-counting in
`dq_report.json`.

---

## Surrogate Key Design

```
sk = CAST(CONV(SUBSTRING(SHA2(natural_key, 256), 1, 15), 16, 10) AS BIGINT)
```

- **Deterministic**: same input → same output across re-runs.
- **No global sort**: each row is computed independently.
- **Fits BIGINT**: 15 hex chars ≈ 60 bits of entropy.
- **Collision-safe at scale**: < 1 in 10⁹ probability for 3 M rows.

---

## Resource Budget

Sized for `docker run -m 4g --cpus=2`:

| Resource | Limit | Configuration |
|---|---|---|
| RAM | 4 GB | `spark.driver.memory=1g`, `spark.executor.memory=2g` (~1 GB headroom for Python + OS) |
| CPU | 2 vCPU | `spark.master=local[2]`, `spark.default.parallelism=2` |
| Shuffle partitions | 4 | `spark.sql.shuffle.partitions=4` |
| Adaptive query exec | On | `spark.sql.adaptive.enabled=true` |
| Coalesce partitions | On | `spark.sql.adaptive.coalescePartitions.enabled=true` |
| Disk (output) | unbounded | Delta; OPTIMIZE + ZORDER opt-in via `optimize_gold` flag |

### Config resolution

The pipeline looks for `pipeline_config.yaml` and `dq_rules.yaml` in this order:
1. Path from `PIPELINE_CONFIG` / `DQ_RULES_PATH` env vars (if set and exists)
2. `/data/config/` (operator-provided via the data mount)
3. `/app/config/` (baked into the image — always present)

This means the standard harness command works whether or not configs are
included in the data mount.

---

## Performance Targets

Indicative wall-clock times on the scoring container (2 GB / 2 vCPU). Actual
times depend on dataset variant.

| Stage | Stage 1 (5k txn) | Stage 2 (3M txn) |
|---|---|---|
| Bronze ingestion | < 30 s | ~3 min |
| Silver transformation | < 30 s | ~5 min |
| Gold provisioning | < 30 s | ~4 min |
| Stage 3 streaming (12 files) | n/a | ~3–4 min |
| **Total wall-clock budget** | **< 2 min** | **< 20 min** |

---

## Observability

Every log line is a single-line JSON object on stdout:

```json
{"ts":"2026-04-27T16:25:00Z","level":"INFO","logger":"pipeline.transform",
 "event":"transform.silver.written","table":"transactions",
 "path":"/data/output/silver/transactions"}
```

This is parseable directly by Splunk, ELK, Datadog, and CloudWatch. Override
verbosity via `PIPELINE_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR`.

---

## Reliability Features

- **Per-file streaming error recovery** (`stream_ingest.py`) — a corrupt
  micro-batch is logged and recorded in `/tmp/stream_failed.txt`; the loop
  continues processing the remaining files.
- **Schema enforcement** on the two stable CSVs with explicit `StructType`
  and PERMISSIVE mode + `_corrupt_record` quarantine.
- **Idempotent writes** — Delta `MERGE INTO` for streaming; deterministic
  surrogate keys mean batch re-runs produce identical Gold tables.
- **DQ flag priority chain** prevents double-counting.
- **Q2 structurally guaranteed** — `dim_accounts` is built by inner-joining
  to `dim_customers`, so orphaned customer references can never reach Gold.

---

## Production Roadmap

Where this pipeline would evolve in a Nedbank production deployment:

1. **Schema registry** — replace inline `StructType` with Avro/Protobuf
   schemas managed in Confluent Schema Registry, validated at ingest.
2. **Native Spark Structured Streaming** — replace the polling loop with
   `readStream("delta")` + `foreachBatch` for sub-second latency and
   guaranteed exactly-once semantics.
3. **Kafka source** — add a Kafka source for `stream_ingest.py` so the same
   code path serves real-time events.
4. **Unity Catalog / Hive Metastore** — register all Delta tables for SQL
   discovery and column-level lineage.
5. **Great Expectations** — replace inline DQ flagging with a declarative
   expectation suite, version-controlled alongside the code.
6. **Airflow / Dagster orchestration** — schedule the batch pipeline,
   parameterise re-runs, surface metrics to Grafana.
7. **Original-currency preservation** — store `original_currency` and
   `original_amount` alongside the standardised values for regulatory audit.

---

## Architecture Decision Record

See [`adr/stage3_adr.md`](adr/stage3_adr.md) for the streaming design rationale.
