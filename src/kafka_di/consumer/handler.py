import inspect
from typing import Any, Callable

from kafka_di.di.depends import Depends
from kafka_di.message.models import DecodedMessage


class Handler:
    def __init__(self, func: Callable[..., Any], app: Any | None = None):
        self._func = func
        self._app = app
        self._signature = inspect.signature(func)
        self._dependencies: dict[str, Depends] = {}
        self._accepts_context = 'context' in self._signature.parameters

        for name, param in self._signature.parameters.items():
            if isinstance(param.default, Depends):
                self._dependencies[name] = param.default

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self._func)

    def __call__(self, message: DecodedMessage, context: Any | None = None) -> Any:
        kwargs = {}
        for name, dep in self._dependencies.items():
            if dep.dependency:
                kwargs[name] = dep.dependency()
            else:
                param = self._signature.parameters[name]
                if self._app and hasattr(self._app, 'producer'):
                    annotation = param.annotation
                    if (hasattr(annotation, '__name__') and annotation.__name__ == 'Producer') or (
                        isinstance(annotation, str) and 'Producer' in annotation
                    ):
                        kwargs[name] = self._app.producer

        if self._accepts_context:
            kwargs['context'] = context

        params = list(self._signature.parameters.values())
        if params and params[0].name not in kwargs:
            return self._func(message, **kwargs)

        return self._func(**kwargs)
