from typing import Any, Callable


class Depends:
    def __init__(self, dependency: Callable[..., Any] | None = None, *, use_cache: bool = True):
        self.dependency = dependency
        self.use_cache = use_cache
