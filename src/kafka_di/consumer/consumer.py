import asyncio
import inspect
import threading
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka.aio import AIOConsumer

from kafka_di.consumer.handler import Handler
from kafka_di.consumer.pipeline import ConsumerPipeline
from kafka_di.message.models import DecodedMessage, Message
from kafka_di.middlewares.base import Middleware
from kafka_di.serializers.base import Serializer


class ConsumerContext:
    def __init__(self):
        self._should_commit = True

    def uncommit(self):
        self._should_commit = False

    @property
    def should_commit(self) -> bool:
        return self._should_commit


class Consumer:
    def __init__(
        self,
        configs: dict[str, Any] = {},
        serializer: Serializer | None = None,
        middlewares: Sequence[Middleware] = [],
        app: Any | None = None,
    ):
        self._configs = configs
        self._serializer = serializer
        self._middlewares = list(middlewares)
        self._handlers: dict[str, Handler] = {}
        self._is_running = False
        self._consumer: ConfluentConsumer | AIOConsumer | None = None
        self._app = app

    def subscribe(self, *topics: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            for topic in topics:
                self._handlers[topic] = Handler(func, app=self._app)
            return func

        return decorator

    def set_configs(self, configs: dict[str, Any]):
        self._configs.update(configs)

    def set_serializer(self, serializer: Serializer):
        self._serializer = serializer

    def add_middleware(self, middleware: Middleware):
        self._middlewares.append(middleware)

    def run(self):
        if not self._handlers:
            return

        if any(handler.is_async for handler in self._handlers.values()):
            self._run_async()
            return

        consumer_config = self._configs.copy()
        consumer_config.setdefault('enable.auto.commit', False)

        self._consumer = ConfluentConsumer(consumer_config)
        self._consumer.subscribe(list(self._handlers.keys()))
        self._is_running = True

        try:
            while self._is_running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    # TODO: Add logging
                    continue

                self._process_message(msg)
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
        await self._consumer.subscribe(list(self._handlers.keys()))
        self._is_running = True

        try:
            while self._is_running:
                msg = await self._consumer.poll(timeout=1.0)
                if msg is None or msg.error():
                    continue

                result = self._process_message(msg)
                if inspect.isawaitable(result):
                    await result
        finally:
            self._is_running = False
            if self._consumer:
                await self._consumer.close()

    def stop(self):
        self._is_running = False

    def _process_message(self, kafka_msg):
        topic = kafka_msg.topic()
        handler = self._handlers.get(topic)
        if not handler:
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

        if self._serializer:
            decoded_message = self._serializer.deserialize(message)
        else:
            decoded_message = DecodedMessage(
                topic=message.topic,
                key=message.key,
                value=message.value,
                headers={k: (v.decode() if v is not None else '') for k, v in message.headers.items()},
                partition=message.partition,
                offset=message.offset,
                timestamp=message.timestamp,
                raw_key=message.key,
                raw_value=message.value,
            )

        pipeline = ConsumerPipeline(self._middlewares)
        wrapped_handler = pipeline.wrap(handler)
        context = ConsumerContext()
        result = wrapped_handler(decoded_message, context=context)
        if inspect.isawaitable(result):
            return self._handle_awaitable_result(result, context, kafka_msg)

        if context.should_commit and self._consumer:
            self._consumer.commit(message=kafka_msg)

        return None

    async def _handle_awaitable_result(self, awaitable, context: ConsumerContext, kafka_msg):
        await awaitable
        if context.should_commit and self._consumer:
            if isinstance(self._consumer, AIOConsumer):
                await self._consumer.commit(message=kafka_msg)
            else:
                self._consumer.commit(message=kafka_msg)

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
