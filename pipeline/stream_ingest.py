"""
Stage 3 — Streaming extension: Process micro-batch JSONL files from /data/stream/.

All 12 stream batch files are pre-staged at /data/stream/ when the container
starts. This module processes them in lexicographic (chronological) filename
order, merging results into stream_gold Delta tables.

Output:
  /data/output/stream_gold/current_balances/   — 4 fields; one row per account_id (upsert)
  /data/output/stream_gold/recent_transactions/ — 7 fields; last 50 per account_id

Reliability features:
  - Per-file try/except: a single corrupt micro-batch does not kill the loop.
  - Failed files are recorded in /tmp/stream_failed.txt for post-run audit.
  - Processed files are recorded in /tmp/stream_processed.txt; loop quiesces
    after QUIESCE_SECONDS of inactivity.
"""
from __future__ import annotations

import os
import glob
import time
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

from pipeline.spark_session import load_config, get_or_create_spark
from pipeline.logging_config import get_logger

log = get_logger(__name__)

PROCESSED_STATE_FILE = "/tmp/stream_processed.txt"
FAILED_STATE_FILE = "/tmp/stream_failed.txt"
QUIESCE_SECONDS = 60        # stop after this many seconds without new files
POLL_INTERVAL_SECONDS = 20  # how often to check for new files (meets 5-min SLA)
MAX_RECENT = 50             # retain last N transactions per account
MAX_RETRIES = 2             # retry a failed file up to this many times before giving up


def load_processed_set() -> set:
    if os.path.exists(PROCESSED_STATE_FILE):
        with open(PROCESSED_STATE_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def record_processed(filename: str) -> None:
    with open(PROCESSED_STATE_FILE, "a") as f:
        f.write(filename + "\n")


def record_failed(filename: str, reason: str) -> None:
    with open(FAILED_STATE_FILE, "a") as f:
        f.write(f"{filename}\t{reason}\n")


def normalise_currency_stream(col: F.Column) -> F.Column:
    normalised = F.upper(F.trim(col))
    return F.when(normalised.isin(["ZAR", "R", "RANDS", "710"]), F.lit("ZAR")).otherwise(col)


def parse_stream_events(spark: SparkSession, filepath: str) -> DataFrame:
    """Read a single JSONL stream file and return a standardised DataFrame."""
    df = spark.read.option("multiLine", "false").json(filepath)

    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast(StringType()))
    if "channel" not in df.columns:
        df = df.withColumn("channel", F.lit(None).cast(StringType()))

    df = df.withColumn("currency", normalise_currency_stream(F.col("currency")))
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))

    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat(
                F.col("transaction_date"),
                F.lit(" "),
                F.col("transaction_time"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    if "location" in df.columns:
        df = df.withColumn("_province", F.col("location.province")).drop("location")
    else:
        df = df.withColumn("_province", F.lit(None).cast(StringType()))

    if "metadata" in df.columns:
        df = df.drop("metadata")

    return df


def upsert_current_balances(
    spark: SparkSession, events: DataFrame, table_path: str
) -> None:
    """
    Maintain one row per account_id: running balance updated by net delta each batch.

    Transaction sign convention:
      CREDIT / REVERSAL  →  +amount  (money in)
      DEBIT  / FEE       →  -amount  (money out)
    """
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    now = F.lit(now_str).cast(TimestampType())

    signed_amount = F.when(
        F.col("transaction_type").isin(["CREDIT", "REVERSAL"]),
        F.col("amount"),
    ).otherwise(-F.col("amount"))

    batch_agg = (
        events
        .filter(F.col("account_id").isNotNull() & F.col("amount").isNotNull())
        .groupBy("account_id")
        .agg(
            F.sum(signed_amount).cast(DecimalType(18, 2)).alias("net_delta"),
            F.max("transaction_timestamp").alias("batch_last_ts"),
        )
        .withColumn("updated_at", now)
    )

    if DeltaTable.isDeltaTable(spark, table_path):
        dt = DeltaTable.forPath(spark, table_path)
        (
            dt.alias("tgt")
            .merge(batch_agg.alias("src"), "tgt.account_id = src.account_id")
            .whenMatchedUpdate(
                set={
                    "current_balance": "CAST(tgt.current_balance + src.net_delta AS DECIMAL(18,2))",
                    "last_transaction_timestamp": (
                        "CASE WHEN src.batch_last_ts > tgt.last_transaction_timestamp "
                        "THEN src.batch_last_ts ELSE tgt.last_transaction_timestamp END"
                    ),
                    "updated_at": "src.updated_at",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "account_id": "src.account_id",
                    "current_balance": "src.net_delta",
                    "last_transaction_timestamp": "src.batch_last_ts",
                    "updated_at": "src.updated_at",
                }
            )
            .execute()
        )
    else:
        init_df = batch_agg.select(
            F.col("account_id"),
            F.col("net_delta").alias("current_balance"),
            F.col("batch_last_ts").alias("last_transaction_timestamp"),
            F.col("updated_at"),
        )
        init_df.write.format("delta").mode("overwrite").save(table_path)


def upsert_recent_transactions(
    spark: SparkSession, events: DataFrame, table_path: str
) -> None:
    """Merge new events and retain only the last MAX_RECENT per account_id."""
    now = F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType())

    new_rows = events.select(
        F.col("account_id"),
        F.col("transaction_id"),
        F.col("transaction_timestamp"),
        F.col("amount").cast(DecimalType(18, 2)),
        F.col("transaction_type"),
        F.col("channel"),
        now.alias("updated_at"),
    )

    if DeltaTable.isDeltaTable(spark, table_path):
        dt = DeltaTable.forPath(spark, table_path)
        (
            dt.alias("tgt")
            .merge(
                new_rows.alias("src"),
                "tgt.account_id = src.account_id AND tgt.transaction_id = src.transaction_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            new_rows.write
            .format("delta")
            .mode("overwrite")
            .save(table_path)
        )

    # Retention: keep only the last MAX_RECENT rows per account_id.
    # Skip the delete pass when nothing is over the threshold to avoid
    # an unnecessary full-table scan.
    dt = DeltaTable.forPath(spark, table_path)
    current = dt.toDF()
    w = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())
    to_delete = (
        current
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") > MAX_RECENT)
        .select("account_id", "transaction_id")
    )

    if not to_delete.rdd.isEmpty():
        (
            dt.alias("tgt")
            .merge(
                to_delete.alias("del"),
                "tgt.account_id = del.account_id AND tgt.transaction_id = del.transaction_id",
            )
            .whenMatchedDelete()
            .execute()
        )


def process_file(spark: SparkSession, filepath: str, paths: dict) -> None:
    fname = os.path.basename(filepath)
    log.info("stream.file.start", extra={"file": fname})
    events = parse_stream_events(spark, filepath)

    cb_path = paths["output"]["stream_gold"]["current_balances"]
    rt_path = paths["output"]["stream_gold"]["recent_transactions"]

    upsert_current_balances(spark, events, cb_path)
    upsert_recent_transactions(spark, events, rt_path)
    log.info("stream.file.done", extra={"file": fname})


def run_stream_ingestion() -> None:
    config = load_config()
    spark = get_or_create_spark(config)

    stream_dir = config["paths"]["stream"]["input_dir"]
    paths = config["paths"]

    processed = load_processed_set()
    retry_counts: dict[str, int] = {}  # in-memory; resets per container run
    last_activity = time.time()

    log.info("stream.watcher.start",
             extra={"dir": stream_dir, "quiesce_seconds": QUIESCE_SECONDS,
                    "poll_interval_seconds": POLL_INTERVAL_SECONDS})

    while True:
        all_files = sorted(glob.glob(os.path.join(stream_dir, "*.jsonl")))
        new_files = [f for f in all_files if os.path.basename(f) not in processed]

        if new_files:
            for filepath in new_files:
                fname = os.path.basename(filepath)
                try:
                    process_file(spark, filepath, paths)
                    processed.add(fname)
                    record_processed(fname)
                    retry_counts.pop(fname, None)
                except Exception as exc:
                    # A single bad micro-batch must NOT crash the stream loop.
                    # Retry transient failures up to MAX_RETRIES; only after
                    # exhaustion do we mark the file processed (giving up).
                    attempts = retry_counts.get(fname, 0) + 1
                    retry_counts[fname] = attempts
                    log.error(
                        "stream.file.failed",
                        extra={
                            "file": fname,
                            "attempt": attempts,
                            "max_retries": MAX_RETRIES,
                            "error": str(exc)[:500],
                        },
                        exc_info=True,
                    )
                    if attempts > MAX_RETRIES:
                        log.error(
                            "stream.file.giving_up",
                            extra={"file": fname, "attempts": attempts},
                        )
                        processed.add(fname)
                        record_processed(fname)
                        record_failed(fname, f"after {attempts} attempts: {str(exc)[:500]}")
            last_activity = time.time()
        else:
            idle = time.time() - last_activity
            log.info("stream.idle",
                     extra={"idle_seconds": int(idle),
                            "quiesce_seconds": QUIESCE_SECONDS})
            if idle >= QUIESCE_SECONDS:
                log.info("stream.quiesce")
                break

        time.sleep(POLL_INTERVAL_SECONDS)
