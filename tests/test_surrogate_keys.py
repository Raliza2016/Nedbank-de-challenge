"""
Surrogate key generation must be:
  1. Deterministic — same natural key always yields the same surrogate.
  2. Distinct — different natural keys yield different surrogates (within
     a sample size where SHA-256 collision probability is effectively zero).
  3. Typed BIGINT — required by the dimensional model schema spec.
  4. Null-safe — null natural keys must not crash the pipeline.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pipeline.provision import surrogate_key

# Explicit schema is required for rows that may contain only None — PySpark 3.5
# refuses to infer types from an all-null column (CANNOT_DETERMINE_TYPE).
_NULL_KEY_SCHEMA = StructType([StructField("natural_key", StringType(), True)])


def _materialise(spark, values):
    """Build a one-column DataFrame and apply surrogate_key, returning rows."""
    df = spark.createDataFrame([(v,) for v in values], ["natural_key"])
    return df.select(surrogate_key("natural_key").alias("sk")).collect()


def test_surrogate_key_is_deterministic(spark):
    rows_a = _materialise(spark, ["TXN-0001", "TXN-0002", "TXN-0003"])
    rows_b = _materialise(spark, ["TXN-0001", "TXN-0002", "TXN-0003"])
    assert [r.sk for r in rows_a] == [r.sk for r in rows_b]


def test_surrogate_key_is_distinct_across_inputs(spark):
    rows = _materialise(spark, [f"TXN-{i:06d}" for i in range(1000)])
    sks = [r.sk for r in rows]
    assert len(set(sks)) == 1000, "surrogate keys must be unique across distinct inputs"


def test_surrogate_key_returns_bigint(spark):
    df = spark.createDataFrame([("ACC-001",)], ["natural_key"])
    out = df.select(surrogate_key("natural_key").alias("sk"))
    assert out.schema["sk"].dataType == LongType()


def test_surrogate_key_handles_null(spark):
    df = spark.createDataFrame([(None,)], _NULL_KEY_SCHEMA)
    rows = df.select(surrogate_key("natural_key").alias("sk")).collect()
    assert len(rows) == 1
    assert rows[0].sk is None


def test_surrogate_key_stable_across_string_and_int(spark):
    """Casting to string must happen inside surrogate_key for type-agnostic use."""
    rows = (
        spark.createDataFrame([(12345,)], ["natural_key"])
        .select(surrogate_key("natural_key").alias("sk"))
        .collect()
    )
    rows_str = (
        spark.createDataFrame([("12345",)], ["natural_key"])
        .select(surrogate_key("natural_key").alias("sk"))
        .collect()
    )
    assert rows[0].sk == rows_str[0].sk
