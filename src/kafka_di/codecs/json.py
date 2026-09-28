import dataclasses
import json
from typing import Any


class JsonCodec:
    def encode(self, value: Any, *, target_type: type[Any] | None = None) -> bytes | None:
        if value is None:
            return None

        if hasattr(value, 'model_dump'):
            value = value.model_dump(mode='json')
        elif dataclasses.is_dataclass(value) and not isinstance(value, type):
            value = dataclasses.asdict(value)

        return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()

    def decode(self, value: bytes | None, *, target_type: type[Any] | None = None) -> Any:
        if value is None:
            return None

        if target_type is not None and hasattr(target_type, 'model_validate_json'):
            return target_type.model_validate_json(value)

        decoded = json.loads(value)
        if target_type is None or isinstance(decoded, target_type):
            return decoded
        if dataclasses.is_dataclass(target_type):
            return target_type(**decoded)
        return target_type(decoded)
