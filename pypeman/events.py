"""Application-wide events and their handlers.

An :class:`Event` is a set of handlers (sync or async callables) which
are awaited whenever the event is fired. Handlers are registered with
:meth:`Event.add_handler` (or the :meth:`Event.receiver` decorator) and
removed with :meth:`Event.remove_handler`.

The events pypeman itself fires are the module level ones below; a
plugin typically subscribes to them from its `task_start` and
unsubscribes from its `task_stop` (see :mod:`pypeman.plugins.base`).
"""

import asyncio
import logging

from pypeman.helpers.aio_compat import awaitify

logger = logging.getLogger(__name__)


class Event:
    """
    Asyncio Event class.
    """

    def __init__(self, name=None):
        self.name = name
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

    def _awaitable_handlers(self):
        """
        Yield the handlers as coroutine functions.

        Iterates a snapshot of the handler set, as a handler is allowed
        to (un)subscribe handlers while the event is being fired.
        """
        for handler in tuple(self.handlers):
            yield handler if asyncio.iscoroutinefunction(handler) else awaitify(handler)

    async def fire(self, *args, **kargs):
        """
        Fire current event. All handler are going to be executed.
        """
        for handler in self._awaitable_handlers():
            await handler(*args, **kargs)

    async def fire_safely(self, *args, **kargs):
        """
        Fire current event, shielding the caller from handler errors.

        Every handler is executed even if a previously called one
        raised; exceptions are logged instead of being propagated.
        Meant for events fired from the message path, where a faulty
        handler must not break the processing.
        """
        for handler in self._awaitable_handlers():
            try:
                await handler(*args, **kargs)
            except Exception:
                logger.exception("Error in %s handler %r", self, handler)

    def getHandlerCount(self):
        """
        Return declared handler count.
        """
        return len(self.handlers)

    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, self.name or hex(id(self)))

    __iadd__ = add_handler
    __isub__ = remove_handler
    __call__ = fire
    __len__ = getHandlerCount


channel_change_state = Event("channel_change_state")
