"""
derive_age_band must bucket customers correctly relative to a fixed run_date,
using the boundaries declared in provision.derive_age_band:
  18-25, 26-35, 36-45, 46-55, 56-65, 65+.
"""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

from pipeline.provision import derive_age_band


RUN_DATE = date(2026, 1, 1)

# Explicit schema is required for rows that may contain only None — PySpark 3.5
# refuses to infer types from an all-null column (CANNOT_DETERMINE_TYPE).
_DOB_SCHEMA = StructType([StructField("dob", StringType(), True)])


def _band(spark, dob_iso):
    df = spark.createDataFrame([(dob_iso,)], _DOB_SCHEMA).withColumn(
        "dob", F.col("dob").cast(DateType())
    )
    return df.select(
        derive_age_band(F.col("dob"), RUN_DATE).alias("band")
    ).collect()[0].band


@pytest.mark.parametrize(
    "dob,expected",
    [
        ("2005-06-01", "18-25"),  # ~20
        ("1995-01-01", "26-35"),  # 31
        ("1985-01-01", "36-45"),  # 41
        ("1975-01-01", "46-55"),  # 51
        ("1965-01-01", "56-65"),  # 61
        ("1950-01-01", "65+"),    # 76
    ],
)
def test_age_band_bucketing(spark, dob, expected):
    assert _band(spark, dob) == expected


def test_under_18_returns_null(spark):
    """Children under 18 are out of band — null is the expected sentinel."""
    assert _band(spark, "2020-01-01") is None


def test_null_dob_returns_null(spark):
    assert _band(spark, None) is None
