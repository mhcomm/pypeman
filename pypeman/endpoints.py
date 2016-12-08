import asyncio

all = []

class BaseEndpoint:

    def __init__(self):
        all.append(self)

    @asyncio.coroutine
    def start(self):
        pass

from pypeman.helpers import lazyload

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.http', 'HTTPEndpoint', ['aiohttp'])
wrap.add_lazy('pypeman.contrib.hl7', 'MLLPEndpoint', ['hl7'])


