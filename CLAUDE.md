# CLAUDE.md — databricks-sa-coding

Local-first Databricks development. Code is authored here and deployed to a
**Databricks Free Edition** workspace with Asset Bundles. Read `README.md` for
the full picture; this file is the working agreement.

## Non-negotiables

- **Serverless only.** Free Edition has no classic compute. Never add a
  `job_clusters:`, `new_cluster:`, `node_type_id` or `num_workers` block —
  omitting compute config *is* the serverless config.
- **No outbound internet.** Free Edition egress is restricted to trusted
  domains. Never write code that downloads a dataset, hits a public API, or
  `pip install`s at runtime from an arbitrary index. This is why the data here
  is **generated in-workspace** rather than fetched.
- **Generated or public open data only.** Free Edition is non-commercial use
  only. Never add proprietary, confidential, commercial, or personal data.
- **Never commit `.env`, tokens, or `~/.databrickscfg` contents.**
- **Never write a person's name into this repo.** See below — this one is easy
  to breach by accident.

## No personal names, anywhere in this repo

Nothing committed here may contain the name of a real person: not recruiters,
interviewers, hiring managers, coordinators, panel members, colleagues,
customers or referees. This holds for code, comments, commit messages, docs,
notebooks, test fixtures, synthetic data, and `.lavish/` artifacts alike.

**Why:** this repo is public. Prep material drawn from a private interview pack
routinely carries the names of people who never agreed to appear in a public
GitHub repo, and a search engine does not care that the surrounding context was
flattering.

**How to apply:** use the role instead — "the recruiter", "the coordinator", "the
hiring manager", "the panel", "a Solutions Architect". Roles carry every bit of
meaning the name did for planning purposes. Process detail, round structure,
evaluation criteria and public product facts are all fine to include; they are
common knowledge. It is specifically identities that stay out.

Names belong in the private sibling project (`../ai-engineer-fit/`), not here. If
you are pasting from there, scrub as you paste rather than afterwards.

## Workspace and profile

One workspace, reached through the CLI profile **`nmp-dsci`**. The Makefile
passes `--profile` on every call, so nothing here depends on which profile is
the CLI default.

A stale `DEFAULT` profile pointing at a retired workspace may still exist in
`~/.databrickscfg`. Never fall back to it — if `--profile nmp-dsci` fails,
re-run `make auth` rather than dropping the flag.

## File conventions

- `src/notebooks/**/*.py` are **Databricks source-format notebooks**. They must
  start with `# Databricks notebook source`, separate cells with
  `# COMMAND ----------`, and write markdown as `# MAGIC %md`. Do not convert
  them to `.ipynb` — the whole point is that git sees clean Python. Ruff
  excludes this directory: `spark`, `dbutils` and `display` are injected
  globals and would otherwise flood the lint output.
- **Logic goes in `src/lib/`, not in a notebook.** Every function there is pure
  (DataFrame in, DataFrame out — no I/O, no globals) so it can be tested
  locally. Notebooks orchestrate: read a table, call a transform, write a table,
  `display()` something. If you find yourself writing a `when/otherwise` chain
  in a notebook cell, it belongs in `transforms.py` with a test.
- `dashboards/*.lvdash.json` is deployed **verbatim** — bundle variables are not
  substituted inside it, so table names are fully qualified and hardcoded.
- `src/pipelines/**` holds Lakeflow Declarative Pipeline transformations. They
  read parameters from `spark.conf.get(...)`, set by `configuration:` in the
  pipeline yml — there is no `dbutils.widgets` and no job `parameters:` block.

## Commands

```bash
make install                # CLI + python deps + JDK check (once)
make auth                   # log the CLI in to the workspace
make setup-uc               # create the schemas and landing volume (once)

make test                   # local Spark unit tests, no workspace (~10s)
make lint / make fmt        # ruff
make validate               # check the bundle without deploying
make deploy                 # push notebooks, jobs, pipeline and dashboard
make run                    # run the smoke job, logs stream to the terminal
make ship                   # test -> deploy -> run

make run-pipeline           # run the declarative pipeline variant
make train                  # train + register the demo model to UC
make serve                  # roll the serving endpoint to the @champion version
make sql FILE=src/sql/00_smoke.sql
make pull-repo              # fast-forward the workspace Git folder to origin/main
make pull-dashboard         # pull UI dashboard edits back into the repo
make summary                # deployed resource URLs
```

## Two copies of this repo live in the workspace

They are separate and serve different purposes — confusing them wastes time.

| Copy | Path | Updated by | What it is for |
|---|---|---|---|
| Bundle files | `/Workspace/Users/<you>/.bundle/databricks-sa-coding/dev/files` | `make deploy` | What the deployed jobs and pipeline actually execute |
| Git folder | `/Workspace/Users/<you>/databricks-sa-coding` | `make pull-repo` | Browsing and running notebooks by hand in the UI |

`make deploy` does **not** update the Git folder, and `make pull-repo` does not
change what the jobs run. A Git folder is a clone pinned to a commit; it tracks
the *remote*, so push before pulling it forward or it will fetch a commit that
does not include your latest work.

Note: `databricks repos list` returns an empty page on this workspace even when
a folder exists. `scripts/sync_git_folder.py` looks the folder up by path with
`workspace.get_status` instead — do not "simplify" it back to `repos.list()`.

Prefer `make ship` over calling `databricks` directly, so tests always run first.

`train` and `serve` are not wired into `ship` — they cost serverless minutes and
a Free Edition endpoint slot, so they run on demand.

## When changing a transform

1. Change `src/lib/transforms.py`.
2. Add or update a test in `tests/test_transforms.py` covering the new rule.
3. `make test`.
4. Only then `make deploy && make run`.

Do not verify a logic change by running the job — it is 100× slower and the
failure message is worse.

The same discipline applies to `src/lib/generate.py`, tested in
`tests/test_generate.py`. The generator's contract is that it *injects* defects
(duplicates, late arrivals, nulls); a change that quietly removes one leaves the
pipeline tested against nothing interesting, so those defects are asserted.

The declarative pipeline imports the *same* `clean_events` and `daily_revenue`
rather than re-implementing them, so a rule change in `transforms.py` reaches
both the job and the pipeline at once — there is no second copy to keep in sync.
The pipeline reaches `src/lib` via `sa_coding.lib_root` in its `configuration:`
block, because a pipeline has no notebook-relative working directory.

The expectations in the pipeline are a runtime guard, not the primary defence:
`clean_events` already removes those rows and the unit tests prove it. If an
expectation ever fires, there is a bug the tests did not catch.

## When changing the dashboard

Editing `.lvdash.json` by hand is fine for small things (a title, a query) but
layout and formatting are far quicker in the UI. If you change it in the UI, run
`make pull-dashboard` before committing or the next deploy will conflict.

## Free Edition limits worth knowing before you hit them

| Limit | Consequence |
|---|---|
| No classic compute | Serverless only; any cluster config fails |
| 2 custom serving endpoints | A third fails the *whole* deploy, not just the endpoint |
| Restricted egress | Runtime downloads and `pip install` from arbitrary indexes fail |
| Fixed catalog (`workspace`) | No `CREATE CATALOG`; schemas are the unit of separation |
| Finite free credits | This is a fresh account after a previous one was exhausted — do not leave a continuous pipeline or a non-scale-to-zero endpoint running |

## Context

This repo backs preparation for a Databricks Solutions Architect interview loop —
specifically the coding round: a 60-minute open-book pair-programming session on
Free Edition where a dataset is generated from a prompt on the day and a pipeline
or analytical solution is built against it, live.

That shapes the code. It should be **explainable out loud**: prefer the approach
whose trade-off is easy to articulate over the clever one, and put the *why* in
a comment, because the why is what gets asked. Anything that would cost UI
fiddling on the day — auth, schemas, a bundle that deploys, a dashboard that
renders — is already done here so the hour goes on the problem instead.
