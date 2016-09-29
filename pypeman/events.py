import asyncio

class Event:
    def __init__(self):
        self.handlers = set()

    def add_handler(self, handler):
        self.handlers.add(handler)
        return self

    def remove_handler(self, handler):
        try:
            self.handlers.remove(handler)
        except:
            raise ValueError("Handler is not handling this event, so cannot unhandle it.")
        return self

    def receiver(self, handler):
        self.add_handler(handler)
        return handler

    @asyncio.coroutine
    def fire(self, *args, **kargs):
        for handler in self.handlers:
            if not asyncio.iscoroutinefunction(handler):
                handler = asyncio.coroutine(handler)

            yield from handler(*args, **kargs)

    def getHandlerCount(self):
        return len(self.handlers)

    __iadd__ = add_handler
    __isub__ = remove_handler
    __call__ = fire
    __len__  = getHandlerCount


channel_change_state = Event()

