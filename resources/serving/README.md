# Why there is no serving endpoint in this bundle

A `model_serving_endpoints` resource must name a model **version** that already
exists — serving config cannot follow a Unity Catalog alias on its own. On a
fresh workspace nothing is registered yet, so a first `make deploy` would fail
on the endpoint and take every unrelated resource in the same deploy down with
it.

So the endpoint is created *after* the model exists, by `make serve`
(`scripts/deploy_endpoint.py`), which reads the `@champion` alias and points the
endpoint at whatever version it currently resolves to. Re-running it after a
retrain is how a new champion goes live.

Free Edition caps custom serving endpoints at **two**, so treat those slots as a
budget: an agent endpoint and a model endpoint, or two models — not more.

If this ever needs to move into the bundle, the shape is:

```yaml
resources:
  model_serving_endpoints:
    demo_estimator_endpoint:
      name: sa-coding-demo-estimator
      config:
        served_entities:
          - name: demo-estimator
            entity_name: ${var.catalog}.${var.ml_schema}.demo_estimator
            entity_version: "1"       # <- the part that has to already exist
            workload_size: Small
            scale_to_zero_enabled: true
```

`scale_to_zero_enabled: true` means the endpoint costs nothing between queries
and the first request after idle pays a cold start — the trade-off worth saying
out loud rather than hiding.
