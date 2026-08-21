# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — generate the synthetic feed
# MAGIC
# MAGIC Bronze is **append-only and lossless**: whatever the source produced,
# MAGIC stored as-is plus ingest metadata. No typing, no repair, no dedupe — that
# MAGIC is silver's job, in code that has unit tests. If a silver rule turns out
# MAGIC to be wrong, we rebuild from here instead of re-fetching.
# MAGIC
# MAGIC The feed is generated rather than downloaded on purpose. Free Edition
# MAGIC restricts outbound internet to trusted domains, so a notebook that
# MAGIC `wget`s a dataset fails in the workspace even though it worked locally.
# MAGIC Generating also means the data is reproducible: same `seed`, same rows.
# MAGIC
# MAGIC The generator lives in `src/lib/generate.py` and is written with the
# MAGIC DataFrame API rather than row-by-row on the driver — 50k rows and 50M
# MAGIC rows run the identical code path, which is the answer when someone asks
# MAGIC how this scales.

# COMMAND ----------

import sys

# The bundle syncs the whole repo to the workspace, so `src/lib` is a sibling
# directory of `src/notebooks`. Prepending it is how a notebook imports the
# tested library code rather than re-implementing it in a cell.
sys.path.insert(0, "../..")
sys.path.insert(0, "..")

from lib.generate import make_events  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sa_coding")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("rows", "50000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
rows = int(dbutils.widgets.get("rows"))

target = f"`{catalog}`.`{schema}`.bronze_events"

# COMMAND ----------

events = make_events(spark, rows=rows, seed=42)

# overwrite, not append: this is a regenerated source, so appending would just
# stack identical copies on every run. A real feed would use Auto Loader with a
# checkpoint here instead — same table, different write mode.
(
    events.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)

print(f"wrote {events.count():,} rows to {target}")

# COMMAND ----------

display(spark.table(target).limit(20))
