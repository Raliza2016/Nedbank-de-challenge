"""
Shared pytest fixtures.

A single SparkSession is created for the whole test session and reused across
modules — Spark startup is expensive (5–10 s) and stateless tests do not
require isolation between cases.
"""
from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Make `pipeline.*` importable when pytest is run from the project root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Lightweight test SparkSession with Delta extensions enabled."""
    builder = (
        SparkSession.builder
        .appName("nedbank-de-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
