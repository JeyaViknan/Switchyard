VENV := .venv
PY   := $(VENV)/bin/python

.PHONY: install test lint typecheck check bench-baseline bench up down clean

install:
	python3.14 -m venv $(VENV)
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e ".[dev,bench]"

lint:
	$(VENV)/bin/ruff check src tests

typecheck:
	$(VENV)/bin/mypy src/switchyard/adapters src/switchyard/types.py

test:
	$(PY) -m pytest -q

check: lint typecheck test

# Regenerates every figure from scratch. If a plot in the report cannot be
# reproduced by this target, it is not evidence.
bench: bench-baseline


	$(PY) -m switchyard.bench.baseline --rates 2,5,10,20,40 --duration 12 --warmup 2

up:
	docker compose up --build -d

down:
	docker compose down -v

clean:
	rm -rf results/*.parquet .pytest_cache .ruff_cache .mypy_cache
