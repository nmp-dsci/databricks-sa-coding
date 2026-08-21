# databricks-sa-coding

Local-first Databricks development against a **Free Edition** workspace. Code is
authored on a laptop in a normal editor with normal tests, and deployed with
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/). The
workspace runs the code; it does not store it.

The scaffold deploys one of each thing, so every path is proven before it is
needed:

| Path | Resource | Run it |
|---|---|---|
| **Job** | `sa-coding-smoke` — generate → transform → checks, serverless | `make run` |
| **Pipeline** | `sa-coding-smoke-pipeline` — the same work as a Lakeflow Declarative Pipeline | `make run-pipeline` |
| **Dashboard** | `SA Coding — Smoke`, deployed from JSON in the repo | `make deploy` |
| **ML** | `sa-coding-ml-train` — MLflow → Unity Catalog registry → `@champion` alias | `make train` |
| **Serving** | Endpoint rolled to whatever `@champion` resolves to | `make serve` |
| **AI functions** | `ai_classify` / `ai_query` from SQL, no endpoint needed | `make sql FILE=src/sql/01_ai_functions.sql` |

---

## Quick start

```bash
make install     # Databricks CLI, python deps, JDK check
make auth        # OAuth login (falls back to a PAT), writes the nmp-dsci profile
make setup-uc    # create the schemas and landing volume — once per workspace
make ship        # test -> deploy -> run
```

`make help` lists everything.

---

## How it fits together

```
                    laptop                                    workspace
  ┌─────────────────────────────────────┐        ┌──────────────────────────────┐
  │ src/lib/*.py        pure functions  │        │                              │
  │   ↑ tested by tests/*.py (local     │        │   workspace.sa_coding        │
  │     Spark, no workspace, ~10s)      │        │     bronze_events            │
  │                                     │        │     silver_events            │
  │ src/notebooks/*.py  orchestration   │        │     gold_daily_revenue       │
  │ src/pipelines/**    declarative     │──DAB──▶│                              │
  │ dashboards/*.json   verbatim        │ deploy │   workspace.sa_coding_dp     │
  │ resources/**/*.yml  what to deploy  │        │     (pipeline's own copies)  │
  │                                     │        │                              │
  │ databricks.yml      bundle + vars   │        │   workspace.sa_coding_ml     │
  └─────────────────────────────────────┘        │     demo_estimator @champion │
                                                 └──────────────────────────────┘
```

The split that matters: **rules live in `src/lib/`, orchestration lives in
notebooks.** A rule with a test beside it can be changed and verified in ten
seconds. The same rule inside a notebook cell takes a five-minute job run to
check and reports failure as a stack trace in a driver log.

### Three schemas, on purpose

| Schema | Owner | Why it is separate |
|---|---|---|
| `sa_coding` | the job | the working medallion tables |
| `sa_coding_dp` | the declarative pipeline | so it can never overwrite the job's tables — the two are a side-by-side comparison, not a replacement |
| `sa_coding_ml` | the ML loop | so a retrain can never touch a mart a dashboard is reading |

On Free Edition the catalog is fixed at `workspace`, so the schema is the only
unit of separation available. That constraint is worth stating out loud rather
than working around.

---

## The data is generated, not downloaded

Free Edition restricts outbound network egress to trusted domains, so a notebook
that fetches a dataset works locally and fails in the workspace. Everything here
is generated in-workspace by `src/lib/generate.py`.

That turns out to be better anyway:

- **Reproducible.** Same seed, same rows. A downstream diff means the transform
  changed, not the input.
- **Scalable by a parameter.** 50k rows and 50M rows run the identical code
  path, because the generator uses the DataFrame API rather than looping on the
  driver.
- **Realistically broken.** It injects the three defects a real feed has —
  duplicate `event_id`s from at-least-once delivery, late-arriving events, and
  null/implausible values. A pipeline that has never seen those is not a
  pipeline, and the tests assert the defects are still there.

---

## The transforms, and the arguments behind them

Two functions, in `src/lib/transforms.py`. Both are short; the reasoning is the
interesting part.

**`clean_events` — filter, *then* dedupe.**
Order matters. If the dedupe ran first, a corrupt later copy of an event would
win its window and then be dropped by the filter, losing the good copy with it.
The window keeps the **latest ingest** rather than the first: a re-delivery is
more often a correction than an accident, so last-write-wins is the safer
default. The tie-break on `event_ts` is what makes the result deterministic when
a source re-sends within the same millisecond — without it, silver churns
between runs that read identical bronze.

**`daily_revenue` — group by event date, not ingest date.**
A late-arriving order belongs to the day it happened; grouping on ingest date
would pile a backfill onto a day it had nothing to do with. The cost is real and
should be said rather than hidden: **yesterday's number can still move.**
`late_orders` is carried as its own column precisely so that a figure which
moved after the fact is explainable instead of alarming.

---

## Job or pipeline?

Both are deployed, running the **same** `clean_events` and `daily_revenue` from
`src/lib` — the pipeline imports them rather than re-implementing them. So the
comparison is purely about the framework, and verified rather than asserted: the
symmetric difference between the two gold marts is zero rows.

|  | Job (`src/notebooks/`) | Declarative pipeline (`src/pipelines/`) |
|---|---|---|
| Dependencies | declared in yml as a task graph | inferred from `dlt.read` calls |
| Quality checks | a notebook of assertions that fails the run | `@dlt.expect_*`, tracked per-run in the UI |
| Incremental | whatever you write | streaming tables and MV refresh, built in |
| Flexibility | anything Python can do | constrained to the pipeline's model |
| Best when | orchestration is heterogeneous — SQL, ML, an API call | the work really is a dependency graph of tables |

The honest summary: the pipeline gives you lineage, expectations and incremental
refresh for free, and takes away control. Neither is the correct answer in
general; which one fits depends on how much of the work is table-to-table.

One constraint shaped the pipeline as written. `01_generate.py` writes bronze
with `mode("overwrite")`, so the whole table is regenerated every run — and a
streaming read over that fails on the overwrite commit, because streaming needs
an append-only source. Materialized views are the right shape for a full-refresh
source. If bronze became a real append-only feed (Auto Loader over the landing
volume), the right shape changes to a streaming table fed by
`create_auto_cdc_flow(keys=["event_id"], sequence_by=struct("ingest_ts", "event_ts"))`,
which is the declarative equivalent of the dedupe window inside `clean_events`.

---

## The ML loop

`make train` runs one job that does four things, and the fourth is the one that
matters:

1. Build calendar and lag features from the gold mart. Not order counts —
   predicting a day's revenue from its own order count is circular.
2. Split **without shuffling**. This is a time series; a random split leaks the
   future into training and produces a validation score that cannot be
   reproduced in production.
3. Log to MLflow with a signature, and register into **Unity Catalog**
   (`mlflow.set_registry_uri("databricks-uc")` — the line that decides this).
   UC models are governed objects with lineage and grants; workspace-registry
   models are not.
4. Move the `@champion` **alias only if the new version is genuinely better.**
   Registering and promoting are separate decisions. Conflating them is how a
   worse model ships.

`make serve` then points the endpoint at whatever `@champion` currently resolves
to. It is a script rather than a bundle resource because a serving resource must
name a concrete model *version*, so a first deploy into an empty workspace would
fail on it and take every unrelated resource down with it. See
`resources/serving/README.md`.

---

## Free Edition limits

| Limit | What it means here |
|---|---|
| No classic compute | Serverless only. There is no `job_clusters:` block anywhere in this repo, and that omission *is* the config. |
| 2 custom serving endpoints | A third fails the whole deploy, not just the endpoint. Treat the slots as a budget. |
| Restricted egress | No runtime downloads, no `pip install` from an arbitrary index. Hence generated data. |
| Fixed catalog | No `CREATE CATALOG`. Schemas are the unit of separation. |
| Finite credits | Nothing continuous, and `scale_to_zero_enabled: true` on anything served. |

---

## Layout

```
databricks.yml              bundle identity, variables, dev/demo targets
resources/
  jobs/                     setup, smoke, ml_train
  pipelines/                the declarative variant
  dashboards/               dashboard resource -> dashboards/*.lvdash.json
  serving/README.md         why the endpoint is a script, not a resource
src/
  lib/                      pure functions — the tested code
  notebooks/                Databricks source-format notebooks (orchestration)
  pipelines/                declarative pipeline transformations
  sql/                      ad-hoc SQL, runnable with `make sql`
dashboards/                 .lvdash.json, deployed verbatim
scripts/                    setup, auth, run_sql, deploy_endpoint
tests/                      local Spark unit tests
.github/workflows/ci.yml    lint + tests on every push; bundle validate if secrets exist
```

---

## Targets

`dev` (default) deploys under `/Workspace/Users/<you>/.bundle/…` with a
`dev_<user>_` name prefix and schedules paused, so a deploy from a laptop never
starts anything running. `demo` deploys unprefixed under `/Workspace/Shared/`
for showing to someone else.

Neither target names a `host:`. The host comes from the CLI profile, so the same
file works against any workspace without an edit.
