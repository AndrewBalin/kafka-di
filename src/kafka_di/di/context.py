from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from kafka_di.consumer.context import ConsumerContext
from kafka_di.message.models import DecodedMessage


@dataclass(slots=True)
class DependencyContext:
    cache: dict[Any, Any] = field(default_factory=dict)
    message: DecodedMessage | None = None
    consumer_context: ConsumerContext | None = None
    app: Any | None = None
    stack: AsyncExitStack | None = None
