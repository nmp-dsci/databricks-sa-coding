# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Unity Catalog setup
# MAGIC
# MAGIC Creates the schemas and the landing volume this bundle writes into.
# MAGIC Run once per workspace (`make setup-uc`); it is idempotent, so re-running
# MAGIC after adding a schema costs nothing.
# MAGIC
# MAGIC Three schemas, not one, and the separation is deliberate:
# MAGIC
# MAGIC | schema | owner | why separate |
# MAGIC |---|---|---|
# MAGIC | `${schema}` | the smoke job | the working medallion tables |
# MAGIC | `${pipeline_schema}` | the declarative pipeline | so it can never write over the job's tables — the two are a comparison, not a replacement |
# MAGIC | `${ml_schema}` | the ML loop | so a retrain can never touch the marts a dashboard reads |
# MAGIC
# MAGIC On Free Edition the catalog is fixed at `workspace` — there is no
# MAGIC `CREATE CATALOG`, which is why the catalog is a parameter to read and
# MAGIC never a thing this notebook creates.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sa_coding")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("pipeline_schema", "sa_coding_dp")
dbutils.widgets.text("ml_schema", "sa_coding_ml")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
pipeline_schema = dbutils.widgets.get("pipeline_schema")
ml_schema = dbutils.widgets.get("ml_schema")

# COMMAND ----------

for name in (schema, pipeline_schema, ml_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{name}`")
    print(f"schema ready: {catalog}.{name}")

# A managed volume: Unity Catalog owns the storage, so there is no external
# location or storage credential to configure — the only kind Free Edition can
# create anyway.
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
print(f"volume ready: /Volumes/{catalog}/{schema}/{volume}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
