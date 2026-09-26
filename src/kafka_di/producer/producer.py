from typing import Any, Sequence

from confluent_kafka import Producer as ConfluentProducer

from kafka_di.app.models import Config
from kafka_di.middlewares import Middleware


class Producer:
    def __init__(self, configs: dict[str, Any] | Config = {}, middlewares: Sequence[Middleware] = []):
        if isinstance(configs, Config):
            configs = configs.serialize_producer()
        self._configs = configs
        self._middlewares = list(middlewares)
        self._producer = ConfluentProducer(self._configs)

    def produce(
        self, topic: str, value: Any, key: Any = None, headers: dict[str, Any] | None = None, delay: float = 0, **kwargs
    ):
        # TODO: Add middleware support for producers.
        self._producer.produce(topic, value=value, key=key, headers=headers, **kwargs)
        self._producer.poll(0)

    def flush(self, timeout: float = -1):
        self._producer.flush(timeout)
