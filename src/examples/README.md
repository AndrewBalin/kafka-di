# Examples

Start Kafka from the repository root:

```bash
docker compose up -d
```

Then run an example with the project environment:

```bash
PYTHONPATH=src uv run python -m examples.app
```

Available examples:

- `app.py` — minimal typed consumer with `JsonCodec`.
- `consumer.py` — low-level access to `DecodedMessage` and `ConsumerContext`.
- `producer_standalone.py` — typed publishing outside a `Kafka` application.
- `producer_depends.py` — injecting the application producer with `Depends()`.
- `dependencies.py` — nested, cached, async, and yield dependencies.
- `middleware.py` — middleware combined with typed events.

The consumer examples keep running until interrupted. Use `producer_standalone.py` to publish an `orders.created`
event for `app.py` or `dependencies.py`.
