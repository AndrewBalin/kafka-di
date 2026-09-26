# Contributing

## Local setup

Kafka DI supports Python 3.13 and newer. Install [uv](https://docs.astral.sh/uv/), then run:

```bash
uv sync --all-groups
make lint
make test
```

Integration tests require a running Docker daemon and can be run with `make test-integration`.

## Pull requests

Keep changes focused, add or update tests where behaviour changes, and ensure `make lint` and `make test` pass before opening a pull request.

## Releases

1. Update `version` in `pyproject.toml` and `kafka_di.__version__` together.
2. Run `make lint`, `make test`, and `make build`.
3. Commit the version change and the updated `uv.lock`.
4. Create a GitHub Release with a tag matching the package version, for example `v0.1.0`.

The `Publish to PyPI` workflow publishes artifacts through PyPI Trusted Publishing. Before the first release, configure a Trusted Publisher in the PyPI project settings with the GitHub owner, repository name, workflow filename `publish.yml`, and environment `pypi`.
