"""Streaming source producer: replays AMLSim's static transactions.csv as a live-arriving
file feed, so txn_stream_ingest.py has something to actually stream from.

This is a demo/drill utility, not a production ingestion path -- collecting the (bounded,
~120K row) dataset to the driver in time-ordered batches and writing plain files with real
pacing delays is the simplest way to produce a *visually convincing* live feed for a
resilience drill or interview walkthrough. A real production source would be the payments
platform's own event stream directly; there'd be no "producer" script at all.
"""
from __future__ import annotations

import csv
import random
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

MALFORMED_INJECTION_RATE = 0.01  # ~1% of rows corrupted on purpose to exercise the dead-letter path
FIELDNAMES = ["sourceNodeId", "targetNodeId", "value", "time"]


def _corrupt_row(row: dict, rng: random.Random) -> dict:
    corruption = rng.choice(("bad_value", "missing_target", "negative_time"))
    corrupted = dict(row)
    if corruption == "bad_value":
        corrupted["value"] = "NOT_A_NUMBER"
    elif corruption == "missing_target":
        corrupted["targetNodeId"] = ""
    elif corruption == "negative_time":
        corrupted["time"] = "-1"
    return corrupted


def drip_feed(
    spark: SparkSession,
    source_csv_path: str,
    target_dir: str,
    rows_per_file: int = 500,
    delay_seconds: float = 0.0,
    inject_malformed: bool = True,
    seed: int = 7,
) -> int:
    """Split source_csv_path (AMLSim's raw transactions.csv) into time-ordered chunk files,
    written one at a time into target_dir, optionally sleeping between writes to simulate
    real arrival pacing. Returns the number of files written.
    """
    df: DataFrame = spark.read.format("csv").option("header", "true").load(source_csv_path)
    ordered_rows = [r.asDict() for r in df.orderBy("time").collect()]

    Path(target_dir).mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    file_count = 0
    for batch_start in range(0, len(ordered_rows), rows_per_file):
        batch = ordered_rows[batch_start : batch_start + rows_per_file]
        if inject_malformed:
            batch = [
                _corrupt_row(row, rng) if rng.random() < MALFORMED_INJECTION_RATE else row
                for row in batch
            ]

        file_count += 1
        out_path = Path(target_dir) / f"txn_batch_{file_count:05d}.csv"
        tmp_path = out_path.with_suffix(".csv.tmp")
        with tmp_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(batch)
        # write-then-rename: the streaming reader must never observe a partially written file
        tmp_path.rename(out_path)

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return file_count


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    source = sys.argv[1]
    target = sys.argv[2]
    delay = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    written = drip_feed(spark_session, source, target, delay_seconds=delay)
    print(f"wrote {written} batch files to {target}")
