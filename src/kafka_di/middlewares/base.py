import inspect


class Middleware:
    def __call__(self, func):
        if inspect.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                return await self._handle_async(func, *args, **kwargs)

            return async_wrapper

        def wrapper(*args, **kwargs):
            return self._handle(func, *args, **kwargs)

        return wrapper

    def _handle(self, func, msg, *args, **kwargs):
        self.handle(msg)
        return func(msg, *args, **kwargs)

    async def _handle_async(self, func, msg, *args, **kwargs):
        self.handle(msg)
        result = func(msg, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def handle(self, msg):
        pass


class ConsumerMiddleware(Middleware):
    pass


class ProducerMiddleware(Middleware):
    pass
