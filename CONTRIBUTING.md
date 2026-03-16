# Contributing to Storey

Storey is an async streaming library for real-time event processing and feature extraction, part of the [mlrun](https://github.com/mlrun/mlrun) ecosystem.

## Prerequisites

- Python 3.11+
- On macOS, you may also need:
  - `brew install lua` — required to install `lupa`, which is used by `fakeredis` to run Redis Lua scripts in unit tests
  - `brew install postgresql` — required to install `psycopg`, which is used by `TimescaleDBTarget`

## Setup

Install runtime and dev dependencies:

```bash
make dev-env
```

Or manually:

```bash
pip install -r requirements.txt
pip install -r dev-requirements.txt
```

## Running Tests

```bash
make test          # unit tests (tests/)
make integration   # integration tests (integration/)
```

Or directly:

```bash
python -m pytest --ignore=integration -rf -v .
```

Integration tests require external services and corresponding environment variables:

| Service      | Environment Variables                                            |
|--------------|------------------------------------------------------------------|
| V3IO         | `V3IO_API`, `V3IO_ACCESS_KEY`                                   |
| Kafka        | `KAFKA_BROKERS`                                                  |
| Redis        | `MLRUN_REDIS_URL`                                                |
| S3           | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_BUCKET`       |
| Azure Blob   | `AZURE_ACCOUNT_NAME`, `AZURE_ACCOUNT_KEY`, `AZURE_BLOB_STORE`   |
| TimescaleDB  | `TIMESCALEDB_DSN`                                                |

Tests for services whose env vars are not set will be skipped.

## Formatting and Linting

```bash
make fmt    # auto-format with black + isort
make lint   # check with flake8 + black + isort
```

Settings: black (line-length 120), isort (profile black), flake8 (max-line-length 120).

## Project Structure

| Directory        | Contents                                      |
|------------------|-----------------------------------------------|
| `storey/`        | Library source code                           |
| `tests/`         | Unit tests                                    |
| `integration/`   | Integration tests (require external services) |
| `bench/`         | Benchmarks                                    |
| `docs/`          | Sphinx documentation                          |

## Branching

The default development branch is `development`.
