lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

fix:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

test:
	PYTHONPATH=src uv run pytest tests/

test-integration:
	PYTHONPATH=src uv run pytest tests/integration/

build:
	uv build
