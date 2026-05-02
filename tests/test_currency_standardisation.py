"""
Currency normalisation must map every recognised ZAR variant to the canonical
string "ZAR" and leave unknown currencies unchanged so that downstream DQ
flagging can mark them as CURRENCY_VARIANT.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.transform import normalise_currency

# Explicit schema is required for rows that may contain only None — PySpark 3.5
# refuses to infer types from an all-null column (CANNOT_DETERMINE_TYPE).
_CURRENCY_SCHEMA = StructType([StructField("currency", StringType(), True)])


def _normalise(spark, value):
    df = spark.createDataFrame([(value,)], _CURRENCY_SCHEMA)
    return df.select(normalise_currency(F.col("currency")).alias("c")).collect()[0].c


@pytest.mark.parametrize("variant", ["ZAR", "zar", "Zar", "zAr", " ZAR "])
def test_canonical_zar_unchanged(spark, variant):
    assert _normalise(spark, variant) == "ZAR"


@pytest.mark.parametrize("variant", ["R", "r", "RANDS", "rands", "Rands", "710"])
def test_known_variants_normalised_to_zar(spark, variant):
    assert _normalise(spark, variant) == "ZAR"


@pytest.mark.parametrize("foreign", ["USD", "EUR", "GBP", "JPY"])
def test_unknown_currencies_pass_through_unchanged(spark, foreign):
    """Unknown currencies must be preserved so the DQ flag can fire downstream."""
    assert _normalise(spark, foreign) == foreign


def test_null_currency_returns_null(spark):
    assert _normalise(spark, None) is None
