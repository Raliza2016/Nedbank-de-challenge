"""
DQ flag priority chain (transform.silver_transactions):

  ORPHANED_ACCOUNT  >  TYPE_MISMATCH  >  DATE_FORMAT  >  CURRENCY_VARIANT

Only one dq_flag may fire per record. Records with multiple issues take the
highest-priority flag. This is the contract the dq_report relies on for
non-double-counted statistics.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, StringType, StructField, StructType,
)


def dq_flag_expr():
    """Replicates the F.when chain from transform.silver_transactions."""
    return (
        F.when(F.col("_is_orphan"), F.lit("ORPHANED_ACCOUNT"))
        .when(F.col("amount").isNull(), F.lit("TYPE_MISMATCH"))
        .when(F.col("transaction_date").isNull(), F.lit("DATE_FORMAT"))
        .when(F.col("_currency_was_variant"), F.lit("CURRENCY_VARIANT"))
        .otherwise(F.lit(None).cast(StringType()))
    )


# Explicit schema — several test cases pass None for amount/txn_date, and
# PySpark 3.5 refuses to infer types from a single-row, all-null column
# (CANNOT_DETERMINE_TYPE). The dq_flag chain only checks isNull(), so the
# numeric type used for amount is irrelevant — DoubleType keeps the table
# small and avoids decimal-vs-float coercion issues.
_FLAG_SCHEMA = StructType([
    StructField("_is_orphan",            BooleanType(), True),
    StructField("amount",                DoubleType(),  True),
    StructField("transaction_date",      DateType(),    True),
    StructField("_currency_was_variant", BooleanType(), True),
])


def _flag(spark, is_orphan, amount, txn_date, currency_variant):
    df = spark.createDataFrame(
        [(is_orphan, amount, txn_date, currency_variant)],
        _FLAG_SCHEMA,
    )
    return df.select(dq_flag_expr().alias("dq_flag")).collect()[0].dq_flag


def test_orphan_wins_over_all_others(spark):
    """An orphaned record with EVERY other issue must still flag as ORPHANED_ACCOUNT."""
    assert _flag(spark, True, None, None, True) == "ORPHANED_ACCOUNT"


def test_type_mismatch_wins_over_date_and_currency(spark):
    assert _flag(spark, False, None, None, True) == "TYPE_MISMATCH"


def test_date_format_wins_over_currency(spark):
    assert _flag(spark, False, 100.0, None, True) == "DATE_FORMAT"


def test_currency_variant_fires_when_only_issue(spark):
    from datetime import date
    assert _flag(spark, False, 100.0, date(2024, 1, 1), True) == "CURRENCY_VARIANT"


def test_clean_record_has_no_flag(spark):
    from datetime import date
    assert _flag(spark, False, 100.0, date(2024, 1, 1), False) is None
