"""Lakeflow Declarative Pipeline — the same silver/gold work as the job.

Read this against `src/notebooks/02_transform.py` and `03_checks.py`. Same
result, and the difference is the whole reason both exist:

  * **Dependencies are declared, not ordered.** There is no task graph in a yml
    file — the pipeline reads `dlt.read` calls and works the DAG out itself.
  * **Quality is a first-class object.** `@dlt.expect_or_fail` is the checks
    notebook, except the results are tracked per-run in the pipeline UI instead
    of being a print statement in a driver log.
  * **The cost is control.** The job can do anything Python can. The pipeline
    trades that for the graph and the expectations.

Parameters arrive through `spark.conf`, set by `configuration:` in
resources/pipelines/smoke_pipeline.yml. A pipeline has no `dbutils.widgets` and
no job `parameters:` block.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window

SOURCE_TABLE = spark.conf.get("sa_coding.source_table")  # noqa: F821
MAX_PLAUSIBLE_AMOUNT = 100_000.0


@dlt.table(
    name="silver_events",
    comment="Deduplicated, validated order events. Latest ingest per event_id wins.",
)
@dlt.expect_or_drop("customer_id_present", "customer_id IS NOT NULL")
@dlt.expect_or_drop("amount_positive", "amount > 0")
@dlt.expect_or_fail("amount_plausible", f"amount <= {MAX_PLAUSIBLE_AMOUNT}")
def silver_events():
    """Expectations replace the filter in `clean_events`; the dedupe stays code.

    `expect_or_drop` quarantines the row and records the drop rate. `expect_or_fail`
    stops the pipeline — reserved for the implausible-amount case, because a
    value that large means the upstream schema changed, and silently dropping
    those rows would hide a broken contract as a slow revenue decline.
    """
    latest = Window.partitionBy("event_id").orderBy(
        F.col("ingest_ts").desc(), F.col("event_ts").desc()
    )
    return (
        spark.readStream.table(SOURCE_TABLE)  # noqa: F821
        .withWatermark("ingest_ts", "1 day")
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dlt.table(
    name="gold_daily_revenue",
    comment="Revenue per event date on realised orders, with late-arrival count.",
)
def gold_daily_revenue():
    return (
        dlt.read("silver_events")
        .filter(~F.col("status").isin("cancelled", "refunded"))
        .withColumn("event_date", F.to_date("event_ts"))
        .groupBy("event_date")
        .agg(
            F.count("*").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
            F.sum(
                F.when(F.datediff(F.col("ingest_ts"), F.col("event_ts")) > 1, 1).otherwise(0)
            ).alias("late_orders"),
        )
    )
