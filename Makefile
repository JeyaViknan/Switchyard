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
        scenario noisy-neighbour provider-outage verify demo dev up down clean

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

down:
	docker compose down -v

clean:
	rm -rf results/*.parquet .pytest_cache .ruff_cache .mypy_cache
