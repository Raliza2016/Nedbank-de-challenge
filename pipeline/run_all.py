"""
Pipeline entry point.

Orchestrates the medallion architecture stages in order:
  1. Ingest    — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables
  4. Stream    — processes /data/stream/ micro-batch files into stream_gold tables
                 (Stage 3 only; skipped if the stream directory has no JSONL files)

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import glob
import os
import sys
import time

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion
from pipeline.spark_session import load_config
from pipeline.logging_config import get_logger

log = get_logger("pipeline.run_all")


def _stream_files_exist(config: dict) -> bool:
    """Return True if there are any JSONL files in the configured stream directory."""
    stream_dir = config.get("paths", {}).get("stream", {}).get(
        "input_dir", "/data/stream"
    )
    try:
        return bool(glob.glob(os.path.join(stream_dir, "*.jsonl")))
    except Exception:
        return False


if __name__ == "__main__":
    pipeline_start_time = time.time()

    try:
        config = load_config()
        stage = str(config.get("pipeline", {}).get("stage", "1"))
        log.info("pipeline.start", extra={"stage": stage})

        run_ingestion()
        run_transformation()
        run_provisioning(pipeline_start_time)

        if _stream_files_exist(config):
            log.info("pipeline.stream.start")
            run_stream_ingestion()
        else:
            log.info("pipeline.stream.skipped",
                     extra={"reason": "no jsonl files in stream dir"})

        elapsed = round(time.time() - pipeline_start_time, 2)
        log.info("pipeline.completed",
                 extra={"stage": stage, "duration_seconds": elapsed})
        sys.exit(0)

    except Exception as exc:
        log.error("pipeline.failed", extra={"error": str(exc)}, exc_info=True)
        sys.exit(1)
