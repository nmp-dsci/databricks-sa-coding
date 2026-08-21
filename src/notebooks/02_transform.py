# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver + gold
# MAGIC
# MAGIC Both transforms are imported from `src/lib/transforms.py`. This notebook
# MAGIC only reads a table, calls a function, and writes a table — if you find
# MAGIC yourself writing a `when/otherwise` chain in a cell here, it belongs in
# MAGIC `transforms.py` with a test beside it.
# MAGIC
# MAGIC - **silver** — drop unusable rows, then keep the latest ingest per
# MAGIC   `event_id`. Filter before dedupe so a corrupt copy never wins.
# MAGIC - **gold** — daily revenue on realised orders, grouped by *event* date
# MAGIC   rather than ingest date, with late arrivals carried as their own column
# MAGIC   so a number that moves after the fact is explainable.

# COMMAND ----------

import sys

sys.path.insert(0, "../..")
sys.path.insert(0, "..")

from lib.transforms import clean_events, daily_revenue  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sa_coding")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"`{catalog}`.`{schema}`"

# COMMAND ----------

bronze = spark.table(f"{prefix}.bronze_events")

silver = clean_events(bronze)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{prefix}.silver_events"
)
print(f"silver_events: {spark.table(f'{prefix}.silver_events').count():,} rows")

# COMMAND ----------

gold = daily_revenue(spark.table(f"{prefix}.silver_events"))
gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{prefix}.gold_daily_revenue"
)
print(f"gold_daily_revenue: {spark.table(f'{prefix}.gold_daily_revenue').count():,} rows")

# COMMAND ----------

display(spark.table(f"{prefix}.gold_daily_revenue").orderBy("event_date"))
