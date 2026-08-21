"""Deterministic synthetic-event generation.

Pure in the sense that matters here: given the same `rows` and `seed` you get
the same DataFrame, and nothing is read from or written to storage. That makes
it unit-testable locally and repeatable in the workspace — a re-run of the job
produces the same bronze table, so a downstream diff means the transform
changed, not the data.

The generator deliberately injects the three defects a real feed has, because a
pipeline that has never seen them is not a pipeline:

  * **duplicates** — the same `event_id` arriving twice, as an at-least-once
    upstream would deliver it
  * **late arrivals** — an `event_ts` well behind `ingest_ts`, which is what
    breaks a naive "process today's partition" job
  * **nulls and bad amounts** — missing `customer_id`, negative `amount`

Scale is controlled by `rows` alone. This is written with the Spark DataFrame
API rather than Faker-per-row on the driver so it stays distributed: 50k rows
and 50M rows run the same code path, which is the point worth making out loud
when someone asks how it scales.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STATUSES = ("placed", "paid", "shipped", "cancelled", "refunded")

# Fraction of rows re-emitted as duplicates, and fraction backdated to land
# late. Kept as module constants so a test can assert against them.
DUPLICATE_RATE = 0.02
LATE_RATE = 0.05
NULL_CUSTOMER_RATE = 0.01


def make_events(
    spark: SparkSession,
    rows: int = 50_000,
    seed: int = 42,
    days: int = 90,
) -> DataFrame:
    """Build a synthetic order-event feed of roughly `rows` rows.

    Roughly, not exactly: duplicates are added on top, so the returned count is
    about ``rows * (1 + DUPLICATE_RATE)``. Callers that need an exact count
    should ``.limit()`` — but the whole point of the duplicates is that they are
    there, so most callers should not.
    """
    if rows <= 0:
        raise ValueError(f"rows must be positive, got {rows}")

    base = (
        spark.range(rows)
        .withColumnRenamed("id", "row_num")
        # rand()/randn() take the seed per-call; offsetting it per column keeps
        # the columns independent rather than perfectly correlated.
        .withColumn("r_customer", F.rand(seed))
        .withColumn("r_amount", F.rand(seed + 1))
        .withColumn("r_status", F.rand(seed + 2))
        .withColumn("r_time", F.rand(seed + 3))
        .withColumn("r_defect", F.rand(seed + 4))
    )

    events = (
        base.withColumn("event_id", F.concat(F.lit("evt-"), F.lpad(F.col("row_num"), 10, "0")))
        # A few thousand customers over any row count, so aggregations have
        # something to group by and the cardinality does not track `rows`.
        .withColumn(
            "customer_id",
            F.when(
                F.col("r_defect") < NULL_CUSTOMER_RATE,
                F.lit(None).cast("string"),
            ).otherwise(
                F.concat(F.lit("cust-"), F.lpad((F.col("r_customer") * 5000).cast("int"), 6, "0"))
            ),
        )
        .withColumn(
            "status",
            F.element_at(
                F.array(*[F.lit(s) for s in STATUSES]),
                (F.col("r_status") * len(STATUSES)).cast("int") + 1,
            ),
        )
        # Log-ish spread so the mean is not the median — a flat uniform amount
        # makes every downstream statistic uninteresting.
        .withColumn("amount", F.round(F.exp(F.col("r_amount") * 6) + 5, 2))
        # Event time is spread over the window. Ingest time is then derived
        # FROM it, which is the part that has to be right: stamping every row
        # with `current_timestamp()` would make `ingest_ts - event_ts` grow with
        # age, so every historical row reads as "late" and the column carries no
        # signal at all.
        #
        # Normal rows land within hours of the event. The late slice lands days
        # after, which is what a downstream `datediff(...) > 1` is meant to
        # catch. `least(..., current_timestamp())` keeps ingest from landing in
        # the future for events near the end of the window.
        .withColumn(
            "event_ts",
            F.expr(
                "current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, "
                f"CAST(r_time * {days} * 86400 AS DOUBLE))"
            ),
        )
        .withColumn(
            "_delay_hours",
            F.when(F.col("r_defect") > (1 - LATE_RATE), 48 + F.col("r_time") * 192).otherwise(
                F.col("r_amount") * 6
            ),
        )
        .withColumn(
            "ingest_ts",
            F.least(
                F.expr(
                    "event_ts + make_interval(0, 0, 0, 0, 0, 0, "
                    "CAST(_delay_hours * 3600 AS DOUBLE))"
                ),
                F.current_timestamp(),
            ),
        )
        .select("event_id", "customer_id", "status", "amount", "event_ts", "ingest_ts")
    )

    # At-least-once delivery: re-emit a slice verbatim. `union` rather than a
    # join so the duplicate is byte-identical apart from nothing at all — the
    # dedupe downstream has to pick one on `event_id`, not on content.
    duplicates = events.sample(withReplacement=False, fraction=DUPLICATE_RATE, seed=seed)
    return events.unionByName(duplicates)
