import asyncio

from pypeman.helpers.aio_compat import awaitify


class Event:
    """
    Asyncio Event class.
    """

    def __init__(self):
        self.handlers = set()

    def add_handler(self, handler):
        """
        Add a new handler for this event.
        """
        self.handlers.add(handler)
        return self

    def remove_handler(self, handler):
        """
        Remove a previously defined handler for this event.
        """
        try:
            self.handlers.remove(handler)
        except Exception:
            raise ValueError("Handler is not handling this event, so cannot unhandle it.")
        return self

    def receiver(self, handler):
        """
        Function decorator to add an handler.
        """
        self.add_handler(handler)
        return handler

    async def fire(self, *args, **kargs):
        """
        Fire current event. All handler are going to be executed.
        """
        for handler in self.handlers:
            if not asyncio.iscoroutinefunction(handler):
                handler = awaitify(handler)

            await handler(*args, **kargs)

    def getHandlerCount(self):
        """
        Return declared handler count.
        """
        return len(self.handlers)

    __iadd__ = add_handler
    __isub__ = remove_handler
    __call__ = fire
    __len__ = getHandlerCount


channel_change_state = Event()
