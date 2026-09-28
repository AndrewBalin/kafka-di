from dataclasses import dataclass

from kafka_di.message.models import DecodedMessage


class ConsumerContext:
    def __init__(self):
        self._should_commit = True

    def uncommit(self):
        self._should_commit = False

    @property
    def should_commit(self) -> bool:
        return self._should_commit


@dataclass(slots=True)
class MessageContext:
    message: DecodedMessage
    consumer: ConsumerContext
