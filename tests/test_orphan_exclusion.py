"""
Q2 contract — fact_transactions must inner-join to dim_accounts so that
LEFT JOIN dim_accounts → dim_customers yields zero orphaned rows.

This test exercises the join logic in provision.build_fact_transactions
end-to-end against a tiny in-memory dataset.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType, DateType,
)

from pipeline.provision import (
    build_dim_accounts,
    build_dim_customers,
    build_fact_transactions,
)


@pytest.fixture
def workspace(tmp_path):
    """Per-test temp directory for Delta paths."""
    return str(tmp_path)


def _silver_customers(spark):
    return spark.createDataFrame(
        [
            ("C1", "F", "Gauteng",      "MID",    "Mass",      550, "VERIFIED", "1990-01-01"),
            ("C2", "M", "Western Cape", "HIGH",   "Affluent",  720, "VERIFIED", "1985-06-15"),
        ],
        ["customer_id", "gender", "province", "income_band",
         "segment", "risk_score", "kyc_status", "dob"],
    )


def _silver_accounts(spark):
    return spark.createDataFrame(
        [
            ("A1", "C1", "CHEQUE", "ACTIVE", "2020-01-01",
             "STANDARD", "ONLINE", 5000.00,  100.00, "2024-12-01"),
            ("A2", "C2", "SAVINGS", "ACTIVE", "2019-06-01",
             "PREMIUM",  "MOBILE", 10000.00, 250.00, "2024-12-15"),
        ],
        ["account_id", "customer_ref", "account_type", "account_status",
         "open_date", "product_tier", "digital_channel",
         "credit_limit", "current_balance", "last_activity_date"],
    )


def _silver_transactions(spark):
    """Two valid transactions + one orphan (account A_GHOST does not exist)."""
    # Amount must be a Decimal — PySpark 3.5 strict type-checking rejects
    # passing a Python float into a DecimalType(18,2) column.
    rows = [
        ("T1", "A1",      "2024-12-20", "12:00:00", "CREDIT", "GROCERIES", None, Decimal("100.00"), "ZAR", "ONLINE", "Gauteng",      None, "2026-04-27 00:00:00"),
        ("T2", "A2",      "2024-12-21", "13:00:00", "DEBIT",  "FUEL",      None, Decimal("50.00"),  "ZAR", "MOBILE", "Western Cape", None, "2026-04-27 00:00:00"),
        ("T3", "A_GHOST", "2024-12-22", "14:00:00", "CREDIT", "RETAIL",    None, Decimal("200.00"), "ZAR", "ONLINE", "Gauteng",      None, "2026-04-27 00:00:00"),
    ]
    schema = StructType([
        StructField("transaction_id",        StringType()),
        StructField("account_id",            StringType()),
        StructField("transaction_date",      StringType()),
        StructField("transaction_time",      StringType()),
        StructField("transaction_type",      StringType()),
        StructField("merchant_category",     StringType()),
        StructField("merchant_subcategory",  StringType()),
        StructField("amount",                DecimalType(18, 2)),
        StructField("currency",              StringType()),
        StructField("channel",               StringType()),
        StructField("province",              StringType()),
        StructField("dq_flag",               StringType()),
        StructField("ingestion_timestamp",   StringType()),
    ])
    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("transaction_date", F.col("transaction_date").cast(DateType()))
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    df = df.withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    return df


def test_orphan_transaction_is_excluded_from_fact(spark, workspace):
    """T3 references account A_GHOST and must NOT appear in fact_transactions."""
    silver_cust = os.path.join(workspace, "silver_customers")
    silver_acc  = os.path.join(workspace, "silver_accounts")
    silver_txn  = os.path.join(workspace, "silver_transactions")
    gold_cust   = os.path.join(workspace, "dim_customers")
    gold_acc    = os.path.join(workspace, "dim_accounts")
    gold_fact   = os.path.join(workspace, "fact_transactions")

    _silver_customers(spark).write.format("delta").mode("overwrite").save(silver_cust)
    _silver_accounts(spark).write.format("delta").mode("overwrite").save(silver_acc)
    _silver_transactions(spark).write.format("delta").mode("overwrite").save(silver_txn)

    from datetime import date
    dim_c = build_dim_customers(spark, silver_cust, gold_cust, date(2026, 4, 27), optimize=False)
    dim_a = build_dim_accounts(spark, silver_acc, gold_acc, dim_c, optimize=False)
    fact  = build_fact_transactions(spark, silver_txn, dim_a, dim_c, gold_fact, optimize=False)

    txn_ids = {row.transaction_id for row in fact.select("transaction_id").collect()}
    assert txn_ids == {"T1", "T2"}, "orphan transaction T3 must be excluded"


def test_q2_zero_orphaned_accounts(spark, workspace):
    """LEFT JOIN dim_accounts → dim_customers must produce zero null customer_sk rows."""
    silver_cust = os.path.join(workspace, "silver_customers_q2")
    silver_acc  = os.path.join(workspace, "silver_accounts_q2")
    gold_cust   = os.path.join(workspace, "dim_customers_q2")
    gold_acc    = os.path.join(workspace, "dim_accounts_q2")

    # Add an account whose customer_ref does NOT exist in customers
    accounts_with_orphan = spark.createDataFrame(
        [
            ("A1", "C1",      "CHEQUE", "ACTIVE", "2020-01-01",
             "STANDARD", "ONLINE", 5000.00, 100.00, "2024-12-01"),
            ("A_BAD", "C_GHOST", "SAVINGS", "ACTIVE", "2019-06-01",
             "PREMIUM", "MOBILE", 10000.00, 250.00, "2024-12-15"),
        ],
        ["account_id", "customer_ref", "account_type", "account_status",
         "open_date", "product_tier", "digital_channel",
         "credit_limit", "current_balance", "last_activity_date"],
    )

    _silver_customers(spark).write.format("delta").mode("overwrite").save(silver_cust)
    accounts_with_orphan.write.format("delta").mode("overwrite").save(silver_acc)

    from datetime import date
    dim_c = build_dim_customers(spark, silver_cust, gold_cust, date(2026, 4, 27), optimize=False)
    dim_a = build_dim_accounts(spark, silver_acc, gold_acc, dim_c, optimize=False)

    # Q2 query equivalent
    orphans = (
        dim_a.alias("a")
        .join(dim_c.alias("c"), F.col("a.customer_id") == F.col("c.customer_id"), "left")
        .filter(F.col("c.customer_sk").isNull())
        .count()
    )
    assert orphans == 0, "dim_accounts must structurally exclude orphaned customer_ids"
