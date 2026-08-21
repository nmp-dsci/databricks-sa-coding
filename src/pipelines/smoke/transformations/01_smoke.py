"""Lakeflow Declarative Pipeline — the same work as the job, declared instead of ordered.

Read this against `src/notebooks/02_transform.py` and `03_checks.py`. It imports
the *same* `clean_events` and `daily_revenue` from `src/lib/transforms.py`, so
the comparison is purely about the framework rather than about two hand-written
copies that will silently drift apart:

  * **Dependencies are declared, not ordered.** There is no task graph in a yml
    file — the pipeline reads the `spark.read.table` calls and works the DAG out
    itself.
  * **Quality is a first-class object.** The expectations below are the checks
    notebook, except results are tracked per-run in the pipeline UI instead of
    being a print statement in a driver log.
  * **The cost is control.** The job can do anything Python can. The pipeline
    trades that for the graph, the expectations and incremental refresh.

**Why materialized views and not streaming tables.** `01_generate.py` writes
bronze with `mode("overwrite")` — the whole table is regenerated every run. A
`spark.readStream.table()` over that fails on the overwrite commit, because a
streaming read needs an append-only source. Materialized views are the correct
shape for a full-refresh source, and saying so is more useful than reaching for
`skipChangeCommits` to paper over the mismatch.

If bronze became a real append-only feed (Auto Loader over the landing volume,
which is what a production version would do), the right shape changes to a
streaming table fed by `dp.create_auto_cdc_flow(keys=["event_id"],
sequence_by=struct("ingest_ts", "event_ts"))` — which is the declarative
equivalent of the dedupe window inside `clean_events`.

Parameters arrive through `spark.conf`, set by `configuration:` in
resources/pipelines/smoke_pipeline.yml. A pipeline has no `dbutils.widgets` and
no job `parameters:` block.
"""

import sys

from pyspark import pipelines as dp

SOURCE_TABLE = spark.conf.get("sa_coding.source_table")  # noqa: F821

# The deployed bundle's file root. A job notebook can import `src/lib` with a
# relative `..`, but a pipeline has no notebook-relative working directory, so
# the path is passed in explicitly and prepended here.
LIB_ROOT = spark.conf.get("sa_coding.lib_root")  # noqa: F821
if LIB_ROOT not in sys.path:
    sys.path.insert(0, LIB_ROOT)

from lib.transforms import MAX_PLAUSIBLE_AMOUNT, clean_events, daily_revenue  # noqa: E402


@dp.materialized_view(
    name="silver_events",
    comment="Deduplicated, validated order events. Latest ingest per event_id wins.",
    cluster_by=["event_ts"],
)
# Belt and braces, not the primary defence. `clean_events` already filters these
# rows out and `tests/test_transforms.py` proves it does. These expectations are
# what turns a silent logic regression into a visible drop count in the pipeline
# UI — if one ever fires, there is a bug the unit tests did not catch.
@dp.expect_all_or_drop(
    {
        "customer_id_present": "customer_id IS NOT NULL",
        "amount_positive": "amount > 0",
    }
)
# Fail, not drop. An amount this large means the upstream schema changed;
# quarantining those rows would hide a broken contract as a slow revenue decline.
@dp.expect_or_fail("amount_plausible", f"amount <= {MAX_PLAUSIBLE_AMOUNT}")
def silver_events():
    return clean_events(spark.read.table(SOURCE_TABLE))  # noqa: F821


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Revenue per event date on realised orders, with late-arrival count.",
)
# The declarative equivalent of the job's `checks` notebook asserting the mart
# has no null revenue. Here it fails the update rather than a downstream task.
@dp.expect_or_fail("revenue_not_null", "revenue IS NOT NULL")
def gold_daily_revenue():
    # Sibling datasets are referenced by bare name, not fully qualified.
    return daily_revenue(spark.read.table("silver_events"))  # noqa: F821
