VENV   := .venv
PY     := $(VENV)/bin/python
# Any interpreter meeting the project's >=3.12 floor. Override if `python3` is
# not the one you want: `make install PYTHON=python3.13`.
PYTHON ?= python3

.PHONY: install test lint typecheck test-all check bench bench-baseline bench-fairness bench-faults \
        scenario noisy-neighbour provider-outage verify dev up down clean

install:
	@$(PYTHON) -c 'import sys; sys.exit(0 if sys.version_info >= (3,12) else 1)' \
	  || { echo "Switchyard needs Python >= 3.12; $(PYTHON) is $$($(PYTHON) -V 2>&1)."; \
	       echo "Try: make install PYTHON=python3.13"; exit 1; }
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e ".[dev,bench]"

lint:
	$(VENV)/bin/ruff check src tests

typecheck:
	$(VENV)/bin/mypy src/switchyard/core src/switchyard/adapters src/switchyard/types.py

test:
	$(PY) -m pytest -q

# Includes the scenario end-to-end tests, which start services and run load.
test-all:
	$(PY) -m pytest -q -m ""

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
