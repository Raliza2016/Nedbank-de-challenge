# Tests

Unit and integration tests for the medallion pipeline.

## Running

From the `pipeline-submission/` directory:

```bash
pip install pytest
pytest
```

## Coverage

| Test file | What it pins |
|---|---|
| `test_surrogate_keys.py` | Determinism, distinctness, BIGINT type, null safety, type-agnostic input |
| `test_currency_standardisation.py` | All ZAR variants → "ZAR"; foreign currencies pass through unchanged |
| `test_date_parsing.py` | ISO, DMY, and epoch formats; null and malformed handling; ISO precedence |
| `test_age_band.py` | Six age buckets relative to a fixed run_date; under-18 and null safety |
| `test_balance_arithmetic.py` | Signed-delta sign convention (CREDIT/REVERSAL +, DEBIT/FEE −) and per-batch aggregation |
| `test_dq_flag_priority.py` | The four-level priority chain ORPHAN > TYPE > DATE > CURRENCY |
| `test_orphan_exclusion.py` | Integration: orphan transactions excluded from `fact_transactions`; Q2 yields 0 orphans |

## Why these tests

Each test pins a behavioural contract that the scoring harness relies on:

- **Surrogate keys** must be stable across re-runs (pipeline can be re-executed safely).
- **Currency normalisation** is the source of truth for the CURRENCY_VARIANT flag count.
- **Date parsing** governs how many records get DATE_FORMAT vs. land cleanly.
- **Age band** is one of the nine `dim_customers` columns; getting the buckets wrong fails the schema check.
- **Balance arithmetic** is the heart of Stage 3 — wrong signs would silently corrupt the streaming feature store.
- **DQ flag priority** prevents double-counting in `dq_report.json`.
- **Orphan exclusion** is the structural guarantee behind Q2.

The tests do not mock Spark — they use a real `SparkSession` with Delta extensions
to catch issues that mocks would hide.
