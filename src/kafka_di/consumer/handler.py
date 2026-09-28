import inspect
from contextlib import AsyncExitStack
from typing import Any, Callable

from kafka_di.consumer.context import ConsumerContext
from kafka_di.di.context import DependencyContext
from kafka_di.di.resolver import DependencyResolver
from kafka_di.message.models import DecodedMessage


class Handler:
    def __init__(self, func: Callable[..., Any], app: Any | None = None, value_type: type[Any] | None = None):
        self._func = func
        self._app = app
        self._value_type = value_type
        self._resolver = DependencyResolver()

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self._func)

    def set_app(self, app: Any):
        self._app = app

    async def __call__(self, message: DecodedMessage, context: ConsumerContext | None = None) -> Any:
        consumer_context = context or ConsumerContext()
        async with AsyncExitStack() as stack:
            dependency_context = DependencyContext(
                message=message,
                consumer_context=consumer_context,
                app=self._app,
                stack=stack,
            )
            kwargs = await self._resolver.resolve_handler_arguments(
                self._func,
                dependency_context,
                value_type=self._value_type,
            )
            result = self._func(**kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
