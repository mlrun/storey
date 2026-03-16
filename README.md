# Storey

Storey is an asynchronous streaming library for real-time event processing and feature extraction. It is part of the [MLRun](https://github.com/mlrun/mlrun) ecosystem.

## Features

- Async event processing with backpressure
- Rich set of built-in transformations: Map, Filter, FlatMap, Batch, Choice, JoinWithTable, and more
- Time-window aggregations with pluggable storage (V3IO, Redis, SQL)
- Sources and targets for files (CSV, Parquet), streams (Kafka, V3IO), and databases (Redis, TimescaleDB)
- Streaming (generator) support in Map steps

## Installation

```bash
pip install storey
```

Optional extras:

```bash
pip install storey[kafka]       # Kafka support
pip install storey[redis]       # Redis support
pip install storey[psycopg]     # TimescaleDB support
```

## Quick Example

```python
from storey import build_flow, SyncEmitSource, Map, Filter, ParquetTarget

controller = build_flow([
    SyncEmitSource(),
    Filter(lambda event: event["amount"] > 0),
    Map(lambda event: {**event, "amount_cents": int(event["amount"] * 100)}),
    ParquetTarget("output.parquet", columns=["user", "amount", "amount_cents"]),
]).run()

controller.emit({"user": "alice", "amount": 9.99})
controller.emit({"user": "bob", "amount": -1.00})
controller.emit({"user": "carol", "amount": 24.50})

controller.terminate()
controller.await_termination()
# output.parquet now contains the two events with positive amounts,
# each enriched with amount_cents.
```

## Documentation

See the [MLRun documentation](https://docs.mlrun.org/en/stable/) and the [storey transformations API reference](https://docs.mlrun.org/en/latest/api/storey.transormations/index.html).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and coding conventions.

## License

Apache License 2.0
