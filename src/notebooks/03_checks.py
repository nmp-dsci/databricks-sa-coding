# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data quality checks
# MAGIC
# MAGIC Runs last in the job so a failed assertion fails the run, and bad numbers
# MAGIC stop here instead of reaching the dashboard. This is the cheap version of
# MAGIC what the declarative pipeline gets for free with `@dlt.expect_or_fail` —
# MAGIC worth having both in the repo, because the comparison is the point.
# MAGIC
# MAGIC Each check states what would be broken if it fired, not just that it
# MAGIC fired.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sa_coding")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"`{catalog}`.`{schema}`"

failures = []


def check(name: str, sql: str, explanation: str) -> None:
    """Assert a scalar SQL expression returns 0, collecting failures."""
    value = spark.sql(sql).collect()[0][0]
    status = "PASS" if value == 0 else "FAIL"
    print(f"[{status}] {name}: {value}")
    if value != 0:
        failures.append(f"{name} = {value} — {explanation}")


# COMMAND ----------

check(
    "silver has no duplicate event_id",
    f"SELECT COUNT(*) FROM (SELECT event_id FROM {prefix}.silver_events "
    "GROUP BY event_id HAVING COUNT(*) > 1)",
    "the dedupe window is wrong; downstream revenue is double-counted",
)

check(
    "silver has no null customer_id",
    f"SELECT COUNT(*) FROM {prefix}.silver_events WHERE customer_id IS NULL",
    "the bronze filter let unattributable orders through",
)

check(
    "silver has no non-positive amounts",
    f"SELECT COUNT(*) FROM {prefix}.silver_events WHERE amount <= 0",
    "refund rows are being counted as orders",
)

check(
    "gold has no null revenue",
    f"SELECT COUNT(*) FROM {prefix}.gold_daily_revenue WHERE revenue IS NULL",
    "an aggregation produced NULL, which a dashboard renders as a blank rather than an error",
)

check(
    "gold covers every silver event date",
    f"""
    SELECT COUNT(*) FROM (
      SELECT DISTINCT to_date(event_ts) AS d FROM {prefix}.silver_events
      WHERE status NOT IN ('cancelled', 'refunded')
      EXCEPT
      SELECT event_date FROM {prefix}.gold_daily_revenue
    )
    """,
    "a day of realised orders is missing from the mart entirely",
)

# COMMAND ----------

if failures:
    raise AssertionError("data quality checks failed:\n  - " + "\n  - ".join(failures))

print("all checks passed")
