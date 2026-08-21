"""Pure DataFrame transforms — the logic that has unit tests.

Every function here takes a DataFrame and returns a DataFrame. No I/O, no
globals, no `spark` reference. That is the whole convention: notebooks
orchestrate (read a table, call a transform, write a table), and anything with a
rule in it lives here where `make test` can reach it in five seconds instead of
a five-minute job run.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Amounts above this are treated as corrupt rather than as a genuinely large
# order. Deliberately a named constant: the number is a business rule, and the
# first question anyone asks about a filter is "why that value".
MAX_PLAUSIBLE_AMOUNT = 100_000.0


def clean_events(df: DataFrame) -> DataFrame:
    """Bronze -> silver: drop unusable rows, then deduplicate on `event_id`.

    Order matters. Filtering first means the dedupe never picks a corrupt row as
    the survivor of a duplicate pair where the other copy was fine.

    Dedupe keeps the **latest ingest** of each `event_id` rather than the first.
    A re-delivery is more often a correction than an accident, so last-write-wins
    is the safer default — and the tie-break on `event_ts` makes the result
    deterministic when a source re-sends within the same millisecond, which is
    what stops the silver table from churning between otherwise identical runs.
    """
    filtered = df.filter(
        F.col("customer_id").isNotNull()
        & F.col("event_id").isNotNull()
        & (F.col("amount") > 0)
        & (F.col("amount") <= MAX_PLAUSIBLE_AMOUNT)
    )

    latest = Window.partitionBy("event_id").orderBy(
        F.col("ingest_ts").desc(), F.col("event_ts").desc()
    )
    return (
        filtered.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def daily_revenue(df: DataFrame) -> DataFrame:
    """Silver -> gold: revenue per event-date, on realised orders only.

    Grouped on `event_ts`, not `ingest_ts`: a late-arriving order belongs to the
    day it happened, otherwise every backfill distorts a day it had nothing to
    do with. The cost is that yesterday's number can still move — which is the
    trade-off to state rather than hide, and the reason `late_orders` is carried
    as its own column instead of being silently folded in.

    `cancelled` and `refunded` are excluded: they are events, but they are not
    revenue.
    """
    realised = df.filter(~F.col("status").isin("cancelled", "refunded"))

    return (
        realised.withColumn("event_date", F.to_date("event_ts"))
        .groupBy("event_date")
        .agg(
            F.count("*").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
            # Landed more than a day after it happened. Worth a column because
            # a spike here explains a revenue number that moved after the fact.
            F.sum(
                F.when(F.datediff(F.col("ingest_ts"), F.col("event_ts")) > 1, 1).otherwise(0)
            ).alias("late_orders"),
        )
        .orderBy("event_date")
    )
