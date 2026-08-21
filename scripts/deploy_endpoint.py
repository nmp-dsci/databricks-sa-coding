#!/usr/bin/env python
"""Create or update the model serving endpoint for the current @champion.

Not part of the bundle deliberately — see resources/serving/README.md. A DAB
`model_serving_endpoints` resource has to name a concrete model *version*, so on
a fresh workspace with nothing registered the first deploy would fail on the
endpoint and take every unrelated resource down with it.

This resolves the `@champion` alias to whatever version it currently points at
and rolls the endpoint to it. Re-run after a retrain that promoted a new
champion; running it when nothing changed is a no-op.
"""

from __future__ import annotations

import os
import sys

try:
    import mlflow
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
except ImportError:  # pragma: no cover
    sys.exit("missing deps — run `uv sync --group dev`")

PROFILE = os.environ.get("PROFILE") or os.environ.get("DATABRICKS_CONFIG_PROFILE", "nmp-dsci")
CATALOG = os.environ.get("CATALOG", "workspace")
ML_SCHEMA = os.environ.get("ML_SCHEMA", "sa_coding_ml")
MODEL = os.environ.get("MODEL_NAME", "demo_estimator")
ENDPOINT = os.environ.get("ENDPOINT_NAME", "sa-coding-demo-estimator")


def main() -> int:
    full_name = f"{CATALOG}.{ML_SCHEMA}.{MODEL}"

    # The CLI profile carries the host and credentials; point both the SDK and
    # MLflow at it rather than requiring DATABRICKS_HOST/TOKEN in the shell.
    client = WorkspaceClient(profile=PROFILE)
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", PROFILE)
    mlflow.set_registry_uri("databricks-uc")

    try:
        version = mlflow.MlflowClient().get_model_version_by_alias(full_name, "champion").version
    except Exception as exc:
        sys.exit(
            f"no @champion alias on {full_name} ({exc}).\n"
            "Run `make train` first — it registers a version and promotes it."
        )

    print(f"{full_name}@champion -> v{version}")

    config = EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                name=MODEL.replace("_", "-"),
                entity_name=full_name,
                entity_version=str(version),
                workload_size="Small",
                # Costs nothing between queries; the first request after idle
                # pays a cold start. The right default for a demo endpoint, and
                # the wrong one for a latency SLA.
                scale_to_zero_enabled=True,
            )
        ]
    )

    existing = {e.name for e in client.serving_endpoints.list()}
    if ENDPOINT in existing:
        print(f"updating existing endpoint {ENDPOINT} ...")
        client.serving_endpoints.update_config_and_wait(
            name=ENDPOINT, served_entities=config.served_entities
        )
    else:
        # Free Edition caps custom endpoints at two, and the failure message
        # ("You've hit the limit for endpoints for free usage") is easy to
        # misread as a permissions problem. Say so up front.
        if len(existing) >= 2:
            print(f"warning: {len(existing)} endpoints already exist — Free Edition allows 2.")
            print(f"         existing: {', '.join(sorted(existing))}")
        print(f"creating endpoint {ENDPOINT} ...")
        client.serving_endpoints.create_and_wait(name=ENDPOINT, config=config)

    url = f"{client.config.host}/ml/endpoints/{ENDPOINT}"
    print(f"\nready: {url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
