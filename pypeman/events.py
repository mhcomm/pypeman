"""Application-wide events and their handlers.

An :class:`Event` is a set of handlers (sync or async callables) which
are awaited whenever the event is fired. Handlers are registered with
:meth:`Event.add_handler` (or the :meth:`Event.receiver` decorator) and
removed with :meth:`Event.remove_handler`. Being a set, it carries no
order: several handlers of the same event run in an undefined one.

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

msg_processing_start = Event("msg_processing_start")
"""Fired by :meth:`pypeman.channels.BaseChannel.handle` on message entry.

Handlers are awaited before the message is stored and processed, and
receive `channel` and `msg` as keyword arguments. `msg` is the very
message the channel is about to process: enriching its `meta` from a
handler is seen by the nodes and by the message store.

Exception: a forked :class:`~pypeman.channels.SubChannel` stores its
copy BEFORE its own events fire, so what its start handlers add to
`msg.meta` is missing from that stored copy (it still carries what the
parent's start handlers added, and store META written by end handlers
is unaffected).
"""

msg_processing_end = Event("msg_processing_end")
"""Fired by :meth:`pypeman.channels.BaseChannel.handle` on message exit.

Handlers are awaited whatever the outcome and receive as keyword
arguments the `channel`, the `msg` the start event was fired with, the
`result` message (`None` if the processing raised) and the raised
`exc` (`None` if it did not).
"""

# Both are fired once per `handle` call: a message going through a
# forked or conditional subchannel fires a pair for the parent channel
# and another one for the subchannel. A conditional subchannel's pair is
# nested in the parent's, a fork's is not: with the default
# `wait_subchans=False` the fork runs as a background task and its whole
# pair fires after the parent's end. Handlers being awaited in the
# message path, a slow handler slows the channel down.
