import contextlib
import inspect
from collections.abc import Callable
from typing import Any, get_type_hints

from kafka_di.consumer.context import ConsumerContext, MessageContext
from kafka_di.di.context import DependencyContext
from kafka_di.di.depends import Depends
from kafka_di.di.errors import DependencyCycleError, UnresolvedParameterError
from kafka_di.message.models import DecodedMessage


class DependencyResolver:
    async def resolve_handler_arguments(
        self,
        func: Callable[..., Any],
        context: DependencyContext,
        *,
        value_type: type[Any] | None = None,
    ) -> dict[str, Any]:
        return await self._resolve_parameters(func, context, value_type=value_type, handler=True, chain=())

    async def resolve(self, dependency: Depends, context: DependencyContext) -> Any:
        return await self._resolve_dependency(dependency, context, chain=())

    async def _resolve_dependency(
        self,
        dependency: Depends,
        context: DependencyContext,
        *,
        chain: tuple[Callable[..., Any], ...],
        annotation: Any = inspect.Parameter.empty,
    ) -> Any:
        provider = dependency.dependency
        if provider is None:
            return self._resolve_builtin(annotation, context)
        if provider in chain:
            path = ' -> '.join(item.__name__ for item in (*chain, provider))
            raise DependencyCycleError(f'Circular dependency detected: {path}')
        if dependency.use_cache and provider in context.cache:
            return context.cache[provider]

        kwargs = await self._resolve_parameters(provider, context, chain=(*chain, provider))
        if inspect.isasyncgenfunction(provider):
            if context.stack is None:
                raise RuntimeError('Dependency context has no exit stack')
            result = await context.stack.enter_async_context(contextlib.asynccontextmanager(provider)(**kwargs))
        elif inspect.isgeneratorfunction(provider):
            if context.stack is None:
                raise RuntimeError('Dependency context has no exit stack')
            result = context.stack.enter_context(contextlib.contextmanager(provider)(**kwargs))
        else:
            result = provider(**kwargs)
            if inspect.isawaitable(result):
                result = await result

        if dependency.use_cache:
            context.cache[provider] = result
        return result

    async def _resolve_parameters(
        self,
        func: Callable[..., Any],
        context: DependencyContext,
        *,
        value_type: type[Any] | None = None,
        handler: bool = False,
        chain: tuple[Callable[..., Any], ...] = (),
    ) -> dict[str, Any]:
        signature = inspect.signature(func)
        try:
            annotations = get_type_hints(func)
        except (NameError, TypeError):
            annotations = {}

        kwargs: dict[str, Any] = {}
        payload_bound = False
        legacy_message_bound = False
        for name, parameter in signature.parameters.items():
            annotation = annotations.get(name, parameter.annotation)
            if isinstance(parameter.default, Depends):
                kwargs[name] = await self._resolve_dependency(
                    parameter.default,
                    context,
                    chain=chain,
                    annotation=annotation,
                )
                continue

            try:
                kwargs[name] = self._resolve_builtin(annotation, context)
                continue
            except UnresolvedParameterError:
                pass

            if handler and name == 'context' and annotation is inspect.Parameter.empty:
                kwargs[name] = context.consumer_context
                continue

            if handler and value_type is not None and not payload_bound:
                if annotation is inspect.Parameter.empty or annotation == value_type:
                    if context.message is None:
                        raise UnresolvedParameterError('Message is not available in this dependency scope')
                    kwargs[name] = context.message.value
                    payload_bound = True
                    continue

            if (
                handler
                and value_type is None
                and not legacy_message_bound
                and parameter.default is inspect.Parameter.empty
            ):
                if context.message is None:
                    raise UnresolvedParameterError('Message is not available in this dependency scope')
                kwargs[name] = context.message
                legacy_message_bound = True
                continue

            if parameter.default is not inspect.Parameter.empty:
                continue
            raise UnresolvedParameterError(f'Cannot resolve parameter {name!r} for {func.__qualname__}')

        return kwargs

    @staticmethod
    def _resolve_builtin(annotation: Any, context: DependencyContext) -> Any:
        if annotation is DecodedMessage:
            if context.message is None:
                raise UnresolvedParameterError('DecodedMessage is not available')
            return context.message
        if annotation is ConsumerContext:
            if context.consumer_context is None:
                raise UnresolvedParameterError('ConsumerContext is not available')
            return context.consumer_context
        if annotation is MessageContext:
            if context.message is None or context.consumer_context is None:
                raise UnresolvedParameterError('MessageContext is not available')
            return MessageContext(message=context.message, consumer=context.consumer_context)

        annotation_name = annotation if isinstance(annotation, str) else getattr(annotation, '__name__', None)
        if annotation_name == 'Producer' and context.app is not None and hasattr(context.app, 'producer'):
            return context.app.producer
        raise UnresolvedParameterError(f'No built-in value for {annotation!r}')
