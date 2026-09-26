from typing import Any, Callable, Sequence

from kafka_di.message.models import DecodedMessage
from kafka_di.middlewares.base import Middleware


class ConsumerPipeline:
    def __init__(self, middlewares: Sequence[Middleware]):
        self._middlewares = middlewares

    def wrap(self, handler: Callable[[DecodedMessage], Any]) -> Callable[[DecodedMessage], Any]:
        wrapped = handler
        for middleware in reversed(self._middlewares):
            wrapped = middleware(wrapped)
        return wrapped
