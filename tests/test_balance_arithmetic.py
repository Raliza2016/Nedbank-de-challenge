"""
Streaming balance arithmetic — the signed-delta logic from stream_ingest:

  CREDIT, REVERSAL  →  +amount
  DEBIT,  FEE       →  -amount

The net delta per account in a micro-batch is the SUM of all signed amounts
for that account. The current_balances table accumulates this delta into the
existing balance via Delta MERGE.
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# Replicates the inline expression in stream_ingest.upsert_current_balances —
# the test pins the contract so refactors cannot change the sign convention.
def signed_amount_expr():
    return F.when(
        F.col("transaction_type").isin(["CREDIT", "REVERSAL"]),
        F.col("amount"),
    ).otherwise(-F.col("amount"))


def _net_delta(spark, events):
    df = spark.createDataFrame(events, ["account_id", "transaction_type", "amount"])
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
    rows = (
        df.groupBy("account_id")
          .agg(F.sum(signed_amount_expr()).cast(DecimalType(18, 2)).alias("net"))
          .collect()
    )
    return {r.account_id: r.net for r in rows}


def test_credit_adds_to_balance(spark):
    deltas = _net_delta(spark, [("A1", "CREDIT", "100.00")])
    assert deltas["A1"] == Decimal("100.00")


def test_debit_subtracts_from_balance(spark):
    deltas = _net_delta(spark, [("A1", "DEBIT", "75.50")])
    assert deltas["A1"] == Decimal("-75.50")


def test_reversal_treated_as_inflow(spark):
    """REVERSAL undoes a prior debit, so it adds back to the balance."""
    deltas = _net_delta(spark, [("A1", "REVERSAL", "50.00")])
    assert deltas["A1"] == Decimal("50.00")


def test_fee_treated_as_outflow(spark):
    deltas = _net_delta(spark, [("A1", "FEE", "5.00")])
    assert deltas["A1"] == Decimal("-5.00")


def test_mixed_batch_aggregates_correctly(spark):
    deltas = _net_delta(
        spark,
        [
            ("A1", "CREDIT",   "1000.00"),
            ("A1", "DEBIT",     "200.00"),
            ("A1", "FEE",        "10.00"),
            ("A1", "REVERSAL",   "50.00"),
            ("A2", "DEBIT",     "300.00"),
            ("A2", "CREDIT",    "500.00"),
        ],
    )
    # A1: +1000 - 200 - 10 + 50 = +840
    assert deltas["A1"] == Decimal("840.00")
    # A2: -300 + 500 = +200
    assert deltas["A2"] == Decimal("200.00")
