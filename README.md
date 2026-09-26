# Kafka DI

Kafka DI is a lightweight framework for event-driven Python applications built on Apache Kafka. It provides declarative consumer handlers, dependency injection, and middleware with a small API inspired by FastAPI.

> **Status:** early-stage project. The PyPI package is `kafka-di`; import it as `kafka_di`.

## Features

- Declarative Kafka consumer handlers.
- Dependency injection through `Depends`.
- Consumer and producer middleware.
- Pydantic-friendly typed configuration and serializers.
- Synchronous and asynchronous message handlers.

## Installation

Install with [uv](https://docs.astral.sh/uv/):

```bash
uv add kafka-di
```

Or with pip:

```bash
pip install kafka-di
```

Kafka DI requires Python 3.13 or newer.

## Quick start

```python
from kafka_di import Kafka
from kafka_di.config import Config

config = Config(
    bootstrap_servers='localhost:9092',
    group_id='orders-service',
)

app = Kafka(configs=config)


@app.consumer.subscribe('orders.created')
def handle_order(message):
    print(f'Received order: {message.value}')


if __name__ == '__main__':
    app.run()
```

## Configuration

Use `Config` for the common settings, or pass a dictionary with any `confluent-kafka` settings.

```python
from kafka_di.config import Config, SecurityProtocols

config = Config(
    bootstrap_servers='localhost:9092',
    group_id='orders-service',
)

sasl_config = Config(
    bootstrap_servers='kafka.example.com:9092',
    group_id='orders-service',
    security_protocol=SecurityProtocols.SASL_SSL,
    sasl_username='service-user',
    sasl_password='secret',
    sasl_mechanisms='PLAIN',
)

raw_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'orders-service',
    'auto.offset.reset': 'earliest',
}
```

## Producing messages

`app.producer` is created on first use.

```python
app.producer.produce('orders.created', value='{"id": 42}', key='42')
app.producer.flush()
```

You can also inject the producer into a handler:

```python
from kafka_di import Depends, Producer


@app.consumer.subscribe('orders.received')
def create_invoice(message, producer: Producer = Depends()):
    producer.produce('invoices.requested', value=message.value)
```

## Consuming messages

Register a consumer through `app.consumer`:

```python
@app.consumer.subscribe('orders.created', 'orders.updated')
def handle_order(message):
    ...
```

Or register a separate consumer:

```python
from kafka_di import Consumer

inventory_consumer = Consumer()


@inventory_consumer.subscribe('inventory.changed')
def update_inventory(message):
    ...


app.register_consumer(inventory_consumer)
```

Async handlers are supported:

```python
@app.consumer.subscribe('orders.created')
async def handle_order(message):
    await persist_order(message.value)
```

## Dependency injection

Use `Depends` to resolve dependencies for a handler:

```python
from kafka_di import Depends


def get_database():
    return DatabaseConnection()


@app.consumer.subscribe('events')
def handle_event(message, database=Depends(get_database)):
    database.save(message.value)
```

## Middleware

Middleware runs before a consumer handler:

```python
from kafka_di.middlewares import ConsumerMiddleware


class LoggingMiddleware(ConsumerMiddleware):
    def handle(self, message):
        print(f'Processing a message from {message.topic}')


app.register_middleware(LoggingMiddleware())
```

## Development

The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync --all-groups
make lint
make test
```

Integration tests start Kafka with Docker and Testcontainers. To run them directly:

```bash
make test-integration
```

For local Kafka and Kafka UI:

```bash
docker compose up -d
```

Kafka is available at `localhost:9092` and Kafka UI at <http://localhost:8080>.

## Releasing

Publishing a GitHub Release runs the PyPI workflow. Before the first release, configure a PyPI Trusted Publisher for the GitHub repository and the `publish.yml` workflow. See [CONTRIBUTING.md](CONTRIBUTING.md) for the release checklist.

## License

Kafka DI is distributed under the [MIT License](LICENSE).
