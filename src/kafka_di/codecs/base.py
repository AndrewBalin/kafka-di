from typing import Any, Protocol


class Codec(Protocol):
    def encode(self, value: Any, *, target_type: type[Any] | None = None) -> bytes | None: ...

    def decode(self, value: bytes | None, *, target_type: type[Any] | None = None) -> Any: ...
