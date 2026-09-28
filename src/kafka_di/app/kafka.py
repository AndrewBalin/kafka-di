import signal
import threading
from typing import Any

from kafka_di.app.models import Config
from kafka_di.codecs import Codec
from kafka_di.consumer.consumer import Consumer
from kafka_di.middlewares.base import ConsumerMiddleware, Middleware, ProducerMiddleware
from kafka_di.producer.producer import Producer


class Kafka:
    def __init__(self, configs: dict[str, Any] | Config | None = None, *, codec: Codec | None = None):
        self.consumer_middlewares: list[Middleware] = []
        self.producer_middlewares: list[Middleware] = []
        self.consumers: list[Consumer] = []
        self._producer: Producer | None = None
        self._configs: dict[str, Any] = {}
        self._codec = codec
        self.set_configs(configs)

    def set_configs(self, configs: dict[str, Any] | Config | None):
        if isinstance(configs, Config):
            self._configs.update(configs.serialize())
        else:
            self._configs.update(configs or {})

    def register_middleware(self, middleware: Middleware):
        if isinstance(middleware, ConsumerMiddleware):
            self.consumer_middlewares.append(middleware)
        elif isinstance(middleware, ProducerMiddleware):
            self.producer_middlewares.append(middleware)
        else:
            # FIXME: Add a custom exception
            raise ValueError('Invalid middleware type')

    def register_consumer(self, consumer: Consumer):
        consumer.set_configs(self._configs)
        consumer.set_app(self)
        if self._codec is not None:
            consumer.set_codec(self._codec)
        self.consumers.append(consumer)

    @property
    def consumer(self):
        consumer = Consumer(configs=self._configs, middlewares=self.consumer_middlewares, app=self, codec=self._codec)
        self.consumers.append(consumer)
        return consumer

    @property
    def producer(self):
        if self._producer is None:
            self._producer = Producer(configs=self._configs, middlewares=self.producer_middlewares, codec=self._codec)
        return self._producer

    def run(self):
        def handle_signal(signum, frame):
            for consumer in self.consumers:
                consumer.stop()

        try:
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
        except ValueError:
            pass

        threads = []
        for consumer in self.consumers:
            thread = threading.Thread(target=consumer.run, daemon=True)
            thread.start()
            threads.append(thread)

        try:
            for thread in threads:
                while thread.is_alive():
                    thread.join(timeout=0.1)
        except (KeyboardInterrupt, SystemExit):
            for consumer in self.consumers:
                consumer.stop()
            for thread in threads:
                thread.join(timeout=2.0)
