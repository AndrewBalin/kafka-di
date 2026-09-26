lint:
	uv run ruff check src/ tests/ scripts/
	uv run ruff format --check src/ tests/ scripts/

fix:
	uv run ruff check --fix src/ tests/ scripts/
	uv run ruff format src/ tests/ scripts/

test:
	PYTHONPATH=src uv run pytest tests/

test-integration:
	PYTHONPATH=src uv run pytest tests/integration/

build:
	uv build
