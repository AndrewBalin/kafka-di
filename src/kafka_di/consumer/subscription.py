from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from kafka_di.codecs import Codec

if TYPE_CHECKING:
    from kafka_di.consumer.handler import Handler


@dataclass(frozen=True, slots=True)
class Subscription:
    topic: str
    value_type: type[Any] | None = None
    key_type: type[Any] | None = None
    codec: Codec | None = None


@dataclass(slots=True)
class RegisteredSubscription:
    subscription: Subscription
    handler: 'Handler'
