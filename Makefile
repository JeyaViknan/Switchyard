VENV := .venv
PY   := $(VENV)/bin/python

.PHONY: install test lint typecheck check bench bench-baseline bench-fairness bench-faults dev up down clean

install:
	python3.14 -m venv $(VENV)
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e ".[dev,bench]"

lint:
	$(VENV)/bin/ruff check src tests

typecheck:
	$(VENV)/bin/mypy src/switchyard/core src/switchyard/adapters src/switchyard/types.py

test:
	$(PY) -m pytest -q

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

# Run the gateway and the synthetic fleet locally against switchyard.toml.
dev:
	$(PY) -m uvicorn switchyard.synthetic.app:app --port 8100 & \
	SWITCHYARD_FLEET_URL=http://127.0.0.1:8100 $(PY) -m uvicorn switchyard.gateway.app:app --port 8000

up:
	docker compose up --build -d

down:
	docker compose down -v

clean:
	rm -rf results/*.parquet .pytest_cache .ruff_cache .mypy_cache
