from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Message:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: dict[str, bytes] = field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None
    timestamp: int | None = None


@dataclass(slots=True)
class DecodedMessage:
    topic: str
    key: Any
    value: Any
    headers: dict[str, str] = field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None
    timestamp: int | None = None
    raw_key: bytes | None = None
    raw_value: bytes | None = None
