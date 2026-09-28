from typing import Any, Sequence

from confluent_kafka import Producer as ConfluentProducer

from kafka_di.app.models import Config
from kafka_di.codecs import Codec
from kafka_di.middlewares import Middleware


class Producer:
    def __init__(
        self,
        configs: dict[str, Any] | Config | None = None,
        middlewares: Sequence[Middleware] | None = None,
        codec: Codec | None = None,
    ):
        if isinstance(configs, Config):
            configs = configs.serialize_producer()
        self._configs = configs or {}
        self._middlewares = list(middlewares or ())
        self._codec = codec
        self._producer = ConfluentProducer(self._configs)

    def produce(
        self, topic: str, value: Any, key: Any = None, headers: dict[str, Any] | None = None, delay: float = 0, **kwargs
    ):
        # TODO: Add middleware support for producers.
        self._producer.produce(topic, value=value, key=key, headers=headers, **kwargs)
        self._producer.poll(0)

    def publish(
        self,
        topic: str,
        value: Any,
        *,
        key: Any = None,
        codec: Codec | None = None,
        value_type: type[Any] | None = None,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        selected_codec = codec or self._codec
        if selected_codec is None:
            return self.produce(topic, value=value, key=key, headers=headers, **kwargs)

        encoded_value = selected_codec.encode(value, target_type=value_type)
        encoded_key = selected_codec.encode(key, target_type=type(key)) if key is not None else None
        return self.produce(topic, value=encoded_value, key=encoded_key, headers=headers, **kwargs)

    def flush(self, timeout: float = -1):
        self._producer.flush(timeout)
