.DEFAULT_GOAL := help
SHELL := /bin/bash

TARGET  ?= dev
PROFILE ?= nmp-dsci
# Extra bundle flags, e.g. make deploy DB_VARS='--var=warehouse_id=abc123'
DB_VARS ?=
DB := databricks --profile $(PROFILE)

# ---------------------------------------------------------------------------
# Local loop — no workspace needed
# ---------------------------------------------------------------------------

.PHONY: install
install: ## Install python deps and the Databricks CLI
	@bash scripts/setup.sh

.PHONY: test
test: ## Run unit tests on local Spark
	@uv run pytest -q

.PHONY: lint
lint: ## Lint and format-check
	@uv run ruff check .
	@uv run ruff format --check .

.PHONY: fmt
fmt: ## Auto-format
	@uv run ruff check --fix .
	@uv run ruff format .

# ---------------------------------------------------------------------------
# Workspace loop
# ---------------------------------------------------------------------------

.PHONY: auth
auth: ## Log the CLI in to the workspace (OAuth, falls back to a PAT)
	@PROFILE=$(PROFILE) bash scripts/auth.sh

.PHONY: whoami
whoami: ## Show which workspace and identity this profile points at
	@$(DB) current-user me | python3 -c 'import json,sys; u=json.load(sys.stdin); print(u.get("userName"))'
	@$(DB) warehouses list

.PHONY: validate
validate: ## Type-check the bundle without deploying
	@$(DB) bundle validate -t $(TARGET) $(DB_VARS)

.PHONY: deploy
deploy: validate ## Push notebooks, jobs, pipelines and dashboards to the workspace
	@$(DB) bundle deploy -t $(TARGET) $(DB_VARS)

.PHONY: setup-uc
setup-uc: ## Create the schemas + landing volume this bundle writes into (run once)
	@$(DB) bundle run setup -t $(TARGET) $(DB_VARS)

.PHONY: run
run: ## Run the smoke job and stream its output here
	@$(DB) bundle run smoke -t $(TARGET) $(DB_VARS)

.PHONY: ship
ship: test deploy run ## test -> deploy -> run, the one command for the inner loop

.PHONY: run-pipeline
run-pipeline: ## Run the Lakeflow Declarative Pipeline
	@$(DB) bundle run smoke_pipeline -t $(TARGET) $(DB_VARS)

.PHONY: train
train: ## Train + register the demo model to Unity Catalog
	@$(DB) bundle run ml_train -t $(TARGET) $(DB_VARS)

.PHONY: serve
serve: ## Create/update the serving endpoint for the registered model
	@PROFILE=$(PROFILE) uv run python scripts/deploy_endpoint.py

.PHONY: sql
sql: ## Run a .sql file on the serverless warehouse. Usage: make sql FILE=src/sql/00_smoke.sql
	@PROFILE=$(PROFILE) uv run python scripts/run_sql.py $(FILE)

.PHONY: sync
sync: ## Live-sync this folder to the workspace on every save (leave running)
	@$(DB) sync --watch . "/Workspace/Users/$$($(DB) current-user me | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')/live/databricks-sa-coding"

.PHONY: pull-dashboard
pull-dashboard: ## Pull UI dashboard edits back into dashboards/*.lvdash.json
	@$(DB) bundle generate dashboard --resource smoke_dashboard -t $(TARGET) --force

.PHONY: summary
summary: ## Print deployed resource URLs
	@$(DB) bundle summary -t $(TARGET) $(DB_VARS)

.PHONY: destroy
destroy: ## Remove everything this bundle deployed
	@$(DB) bundle destroy -t $(TARGET)

.PHONY: help
help:
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
