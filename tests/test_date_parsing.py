"""
parse_date_robust must accept the three canonical formats listed in
config/dq_rules.yaml (date_format rule):
  - yyyy-MM-dd
  - dd/MM/yyyy
  - epoch_int (unix seconds)
and return NULL for unparseable strings so the DATE_FORMAT flag can fire.
"""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.transform import parse_date_robust

# Explicit schema is required for rows that may contain only None — PySpark 3.5
# refuses to infer types from an all-null column (CANNOT_DETERMINE_TYPE).
_DATE_SCHEMA = StructType([StructField("d", StringType(), True)])


def _parse(spark, value):
    df = spark.createDataFrame([(value,)], _DATE_SCHEMA)
    return df.select(parse_date_robust(F.col("d")).alias("parsed")).collect()[0].parsed


def test_iso_yyyy_mm_dd(spark):
    assert _parse(spark, "2024-03-15") == date(2024, 3, 15)


def test_dmy_with_slashes(spark):
    assert _parse(spark, "15/03/2024") == date(2024, 3, 15)


def test_epoch_seconds(spark):
    # 1710460800 = 2024-03-15 00:00:00 UTC
    assert _parse(spark, "1710460800") == date(2024, 3, 15)


def test_invalid_string_returns_null(spark):
    assert _parse(spark, "not-a-date") is None


def test_null_input_returns_null(spark):
    assert _parse(spark, None) is None


def test_iso_takes_precedence_over_dmy(spark):
    """If the string is valid ISO, that format must win over DMY ambiguity."""
    # "2024-03-15" must parse as ISO, not as DMY
    assert _parse(spark, "2024-03-15") == date(2024, 3, 15)
