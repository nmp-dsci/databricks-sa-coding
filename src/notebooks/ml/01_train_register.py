# Databricks notebook source
# MAGIC %md
# MAGIC # ML · Train and register
# MAGIC
# MAGIC The AI/ML deploy path, end to end and deliberately small: fit a model on
# MAGIC the gold mart, log it to MLflow, register it into Unity Catalog, and move
# MAGIC the `@champion` alias only if it beats the incumbent.
# MAGIC
# MAGIC The model itself is not the point — a gradient-boosted regressor on five
# MAGIC features is a placeholder. What matters is that the *loop* is real:
# MAGIC
# MAGIC - **UC registry, not the workspace registry.** `mlflow.set_registry_uri("databricks-uc")`
# MAGIC   is the line that decides this. UC models are governed objects with
# MAGIC   lineage and grants; workspace-registry models are not.
# MAGIC - **Aliases, not stages.** Stages are deprecated. `@champion` is a moving
# MAGIC   pointer, so a serving endpoint or a batch job refers to a role rather
# MAGIC   than to a version number that goes stale.
# MAGIC - **A promotion gate.** A new version is registered every run; the alias
# MAGIC   only moves on a genuine improvement. Registering and promoting are
# MAGIC   separate decisions, and conflating them is how a worse model ships.

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow.models.signature import infer_signature
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

# The line that makes this Unity Catalog rather than the legacy workspace
# registry. Without it, `register_model` silently writes somewhere else.
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sa_coding")
dbutils.widgets.text("ml_schema", "sa_coding_ml")
dbutils.widgets.text("model_name", "demo_estimator")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ml_schema = dbutils.widgets.get("ml_schema")
model_name = dbutils.widgets.get("model_name")

full_model_name = f"{catalog}.{ml_schema}.{model_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Features
# MAGIC
# MAGIC Predicting a day's revenue from its own order count would be circular, so
# MAGIC the features are calendar and lag terms: what we would actually know
# MAGIC *before* the day happened. `toPandas()` is safe here only because gold is
# MAGIC one row per day — at any real size this would stay in Spark and train
# MAGIC distributed, and that limit is worth naming rather than discovering.

# COMMAND ----------

gold = spark.sql(f"""
    SELECT
      event_date,
      revenue,
      dayofweek(event_date)               AS dow,
      weekofyear(event_date)              AS woy,
      month(event_date)                   AS month,
      LAG(revenue, 1) OVER (ORDER BY event_date) AS revenue_lag_1,
      LAG(revenue, 7) OVER (ORDER BY event_date) AS revenue_lag_7
    FROM `{catalog}`.`{schema}`.gold_daily_revenue
    ORDER BY event_date
""").toPandas()

# The first week has no lag-7 value. Dropping is right for a demo; a real model
# would impute and carry a `was_imputed` flag so the model can learn that those
# rows are different.
data = gold.dropna().reset_index(drop=True)
features = ["dow", "woy", "month", "revenue_lag_1", "revenue_lag_7"]

X, y = data[features], data["revenue"]

# shuffle=False: this is a time series, and a random split leaks the future into
# the training set, producing a validation score that cannot be reproduced in
# production. The last 20% of days is the honest holdout.
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
print(f"train={len(X_train)} test={len(X_test)}")

# COMMAND ----------

with mlflow.start_run(run_name="demo_estimator") as run:
    model = GradientBoostingRegressor(n_estimators=200, max_depth=3, random_state=42)
    model.fit(X_train, y_train)

    mae = mean_absolute_error(y_test, model.predict(X_test))
    mlflow.log_params({"n_estimators": 200, "max_depth": 3, "features": ",".join(features)})
    mlflow.log_metric("mae", mae)

    # A signature makes the serving endpoint reject malformed input at the door
    # with a clear error, instead of the model returning a confident number for
    # nonsense. Cheap to add, and the thing you miss when it is absent.
    mlflow.sklearn.log_model(
        model,
        name="model",
        signature=infer_signature(X_train, model.predict(X_train)),
        input_example=X_train.head(3),
    )
    run_id = run.info.run_id

print(f"run {run_id}  MAE={mae:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register, then decide separately whether to promote

# COMMAND ----------

version = mlflow.register_model(f"runs:/{run_id}/model", full_model_name)
print(f"registered {full_model_name} v{version.version}")

client = mlflow.MlflowClient()

try:
    champion = client.get_model_version_by_alias(full_model_name, "champion")
    champion_mae = client.get_run(champion.run_id).data.metrics.get("mae", float("inf"))
except Exception:
    champion, champion_mae = None, float("inf")

# Lower MAE is better. On a tie the incumbent keeps the alias: a swap is not
# free — it invalidates whatever the current endpoint has cached and resets any
# operational confidence in it — so "no worse" is not a good enough reason.
if mae < champion_mae:
    client.set_registered_model_alias(full_model_name, "champion", version.version)
    was = f"{champion_mae:,.2f}" if champion else "none"
    print(f"PROMOTED v{version.version} to @champion (MAE {mae:,.2f} < {was})")
else:
    print(f"kept v{champion.version} as @champion (MAE {champion_mae:,.2f} <= {mae:,.2f})")
