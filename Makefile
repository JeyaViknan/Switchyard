VENV    := .venv
VENV_PY := $(VENV)/bin/python

# Prefer the project venv when it exists, which is the normal local setup.
# Fall back to whatever is on PATH: CI runs `pip install -e .` into the
# runner's own Python and has no .venv, and every target here has to work in
# both places. The bench job failed with "No such file or directory" for two
# months because this was hardcoded -- the same mistake as the test job
# depending on how pytest happened to be launched.
ifneq ($(wildcard $(VENV_PY)),)
  BIN := $(VENV)/bin/
  PY  := $(VENV_PY)
else
  BIN :=
  PY  := python3
endif
# Any interpreter meeting the project's >=3.12 floor. Override if `python3` is
# not the one you want: `make install PYTHON=python3.13`.
PYTHON ?= python3

.PHONY: install test lint typecheck test-all check bench bench-baseline bench-fairness bench-faults \
        scenario noisy-neighbour provider-outage verify demo dev up down clean \
        prod-local prod-local-down secrets guide

install:
	@$(PYTHON) -c 'import sys; sys.exit(0 if sys.version_info >= (3,12) else 1)' \
	  || { echo "Switchyard needs Python >= 3.12; $(PYTHON) is $$($(PYTHON) -V 2>&1)."; \
	       echo "Try: make install PYTHON=python3.13"; exit 1; }
	$(PYTHON) -m venv $(VENV)
	$(VENV_PY) -m pip install -q --upgrade pip
	$(VENV_PY) -m pip install -q -e ".[dev,bench]"

lint:
	$(BIN)ruff check src tests

typecheck:
	$(BIN)mypy src/switchyard/core src/switchyard/adapters src/switchyard/types.py

test:
	$(PY) -m pytest -q

# Everything, including the tests that start real services and drive load.
#
# Two invocations, not one. The slow tests measure real timing, so running
# them in the same process as 370 others means they measure that process's
# accumulated state as much as the system: the load generator's own p99
# scheduling lag rises from 0.7% of request latency to 7.5% (threshold 5%)
# and the run is correctly judged untrustworthy. A fresh interpreter restores
# the quiet conditions a timing measurement needs. Same reason CI does not
# run the benchmarks on a shared runner.
test-all:
	$(PY) -m pytest -q
	$(PY) -m pytest -q -m slow

check: lint typecheck test

# Regenerates every figure from scratch. If a plot in the report cannot be
# reproduced by this target, it is not evidence.
bench: bench-baseline bench-fairness bench-faults

bench-baseline:
	$(PY) -m switchyard.bench.baseline --rates 2,5,10,20,40 --duration 12 --warmup 2

bench-fairness:
	$(PY) -m switchyard.bench.fairness --duration 40 --warmup 5

bench-faults:
	$(PY) -m switchyard.bench.faults --duration 45 --rate 6 --outage-start 12 --outage-end 30

# `make scenario noisy-neighbour` -- the scenario name is a goal Make would
# otherwise try to build, so the known names are declared as no-ops below.
scenario:
	@$(PY) -m switchyard.cli.main scenario $(filter-out scenario,$(MAKECMDGOALS))

noisy-neighbour provider-outage:
	@:

# The whole story in one command: contention, then an outage, then a check of
# the configuration that survived both. About three minutes, no API key.
demo:
	@$(PY) -m switchyard.cli.main scenario noisy-neighbour
	@$(PY) -m switchyard.cli.main scenario provider-outage
	@$(PY) -m switchyard.cli.main verify

# Check that the guarantees in switchyard.toml actually hold.
verify:
	@$(PY) -m switchyard.cli.main verify

# Run the gateway and the synthetic fleet locally against switchyard.toml.
# Runs both services and cleans up the fleet on exit, rather than orphaning it.
dev:
	@trap 'kill 0' EXIT INT TERM; \
	$(PY) -m uvicorn switchyard.synthetic.app:app --port 8100 --log-level warning & \
	SWITCHYARD_FLEET_URL=http://127.0.0.1:8100 \
	  $(PY) -m uvicorn switchyard.gateway.app:create_app --factory --port 8000; \
	wait

up:
	docker compose up --build -d

# Development credentials for the production compose file, written to
# deploy/local/ (git-ignored).
secrets:
	@./scripts/dev-secrets.sh

# The production stack, on a laptop. Same compose file, same topology, same
# health checks and drain timing as the instance -- only the image source and
# the env files differ. This is the rehearsal, and the fallback if AWS or the
# venue wifi is not there on the day.
#
#   console  http://localhost
#   api      http://localhost:8000
#   grafana  http://localhost:3000
PROD_LOCAL_ENV := IMAGE_TAG=switchyard:local SWITCHYARD_ENV_DIR=./local \
                  SWITCHYARD_CONFIG_FILE=./local/switchyard.toml \
                  SWITCHYARD_STATE_DIR=./local/state
PROD_LOCAL := docker compose -f deploy/compose.prod.yml -f deploy/compose.local.yml

prod-local: secrets
	$(PROD_LOCAL_ENV) $(PROD_LOCAL) up --build -d
	@echo "waiting for the stack to come up..."
	@$(PROD_LOCAL_ENV) $(PROD_LOCAL) ps --format 'table {{.Service}}\t{{.Status}}'
	@SMOKE_TENANT_KEY=sk_sy_acme_b6525e6c60405226fc726bdda422dd2f ./deploy/smoke.sh

prod-local-down:
	$(PROD_LOCAL_ENV) $(PROD_LOCAL) down -v

down:
	docker compose down -v

# The interview guide. Twice, because TikZ positioning needs the second pass
# to resolve node references written on the first.
guide:
	cd docs && pdflatex -interaction=nonstopmode -halt-on-error DEVOPS-INTERVIEW-GUIDE.tex >/dev/null
	cd docs && pdflatex -interaction=nonstopmode -halt-on-error DEVOPS-INTERVIEW-GUIDE.tex >/dev/null
	@echo "built docs/DEVOPS-INTERVIEW-GUIDE.pdf"

clean:
	rm -rf results/*.parquet .pytest_cache .ruff_cache .mypy_cache docs/*.aux docs/*.log docs/*.out
