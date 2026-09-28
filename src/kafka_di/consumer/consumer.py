import asyncio
import inspect
import threading
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka.aio import AIOConsumer

from kafka_di.codecs import Codec
from kafka_di.consumer.context import ConsumerContext
from kafka_di.consumer.handler import Handler
from kafka_di.consumer.pipeline import ConsumerPipeline
from kafka_di.consumer.subscription import RegisteredSubscription, Subscription
from kafka_di.message.models import DecodedMessage, Message
from kafka_di.middlewares.base import Middleware
from kafka_di.serializers.base import Serializer


class Consumer:
    def __init__(
        self,
        configs: dict[str, Any] | None = None,
        serializer: Serializer | None = None,
        middlewares: Sequence[Middleware] | None = None,
        app: Any | None = None,
        codec: Codec | None = None,
    ):
        self._configs = configs or {}
        self._serializer = serializer
        self._codec = codec
        self._middlewares = list(middlewares or ())
        self._subscriptions: dict[str, RegisteredSubscription] = {}
        self._is_running = False
        self._consumer: ConfluentConsumer | AIOConsumer | None = None
        self._app = app

    def subscribe(
        self,
        *topics: str,
        value_type: type[Any] | None = None,
        key_type: type[Any] | None = None,
        codec: Codec | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            duplicate_topics = {topic for topic in topics if topics.count(topic) > 1 or topic in self._subscriptions}
            if duplicate_topics:
                duplicates = ', '.join(repr(topic) for topic in sorted(duplicate_topics))
                raise ValueError(f'A handler is already registered for topic(s): {duplicates}')
            for topic in topics:
                subscription = Subscription(
                    topic=topic,
                    value_type=value_type,
                    key_type=key_type,
                    codec=codec,
                )
                self._subscriptions[topic] = RegisteredSubscription(
                    subscription=subscription,
                    handler=Handler(func, app=self._app, value_type=value_type),
                )
            return func

        return decorator

    def set_configs(self, configs: dict[str, Any]):
        self._configs.update(configs)

    def set_serializer(self, serializer: Serializer):
        self._serializer = serializer

    def set_codec(self, codec: Codec):
        self._codec = codec

    def set_app(self, app: Any):
        self._app = app
        for registered in self._subscriptions.values():
            registered.handler.set_app(app)

    def add_middleware(self, middleware: Middleware):
        self._middlewares.append(middleware)

    def run(self):
        if not self._subscriptions:
            return

        if any(registered.handler.is_async for registered in self._subscriptions.values()):
            self._run_async()
            return

        consumer_config = self._configs.copy()
        consumer_config.setdefault('enable.auto.commit', False)

        self._consumer = ConfluentConsumer(consumer_config)
        self._consumer.subscribe(list(self._subscriptions.keys()))
        self._is_running = True

        try:
            while self._is_running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    # TODO: Add logging
                    continue

                self._run_awaitable(self._process_message(msg))
        finally:
            self._is_running = False
            if self._consumer:
                self._consumer.close()

    def _run_async(self):
        self._run_awaitable(self._run_async_loop())

    async def _run_async_loop(self):
        consumer_config = self._configs.copy()
        consumer_config.setdefault('enable.auto.commit', False)

        self._consumer = AIOConsumer(consumer_config)
        await self._consumer.subscribe(list(self._subscriptions.keys()))
        self._is_running = True

        try:
            while self._is_running:
                msg = await self._consumer.poll(timeout=1.0)
                if msg is None or msg.error():
                    continue

                await self._process_message(msg)
        finally:
            self._is_running = False
            if self._consumer:
                await self._consumer.close()

    def stop(self):
        self._is_running = False

    async def _process_message(self, kafka_msg):
        topic = kafka_msg.topic()
        registered = self._subscriptions.get(topic)
        if not registered:
            return

        message = Message(
            topic=topic,
            key=kafka_msg.key(),
            value=kafka_msg.value(),
            headers={k: v for k, v in (kafka_msg.headers() or [])},
            partition=kafka_msg.partition(),
            offset=kafka_msg.offset(),
            timestamp=kafka_msg.timestamp()[1] if kafka_msg.timestamp() else None,
        )

        codec = registered.subscription.codec or self._codec
        if codec:
            key = message.key
            if registered.subscription.key_type is not None:
                key = codec.decode(message.key, target_type=registered.subscription.key_type)
            decoded_message = DecodedMessage(
                topic=message.topic,
                key=key,
                value=codec.decode(message.value, target_type=registered.subscription.value_type),
                headers=self._decode_headers(message.headers),
                partition=message.partition,
                offset=message.offset,
                timestamp=message.timestamp,
                raw_key=message.key,
                raw_value=message.value,
            )
        elif self._serializer:
            decoded_message = self._serializer.deserialize(message)
        else:
            decoded_message = DecodedMessage(
                topic=message.topic,
                key=message.key,
                value=message.value,
                headers=self._decode_headers(message.headers),
                partition=message.partition,
                offset=message.offset,
                timestamp=message.timestamp,
                raw_key=message.key,
                raw_value=message.value,
            )

        pipeline = ConsumerPipeline(self._middlewares)
        wrapped_handler = pipeline.wrap(registered.handler)
        context = ConsumerContext()
        result = wrapped_handler(decoded_message, context=context)
        if inspect.isawaitable(result):
            await result

        if context.should_commit and self._consumer:
            if isinstance(self._consumer, AIOConsumer):
                await self._consumer.commit(message=kafka_msg)
            else:
                self._consumer.commit(message=kafka_msg)

    @staticmethod
    def _decode_headers(headers: dict[str, bytes]) -> dict[str, str]:
        return {key: value.decode() if value is not None else '' for key, value in headers.items()}

    @staticmethod
    def _run_awaitable(awaitable):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(awaitable)
            return

        result: dict[str, BaseException | None] = {'error': None}

        def _runner():
            try:
                asyncio.run(awaitable)
            except BaseException as exc:  # noqa: BLE001
                result['error'] = exc

        thread = threading.Thread(target=_runner)
        thread.start()
        thread.join()

        if result['error'] is not None:
            raise result['error']
