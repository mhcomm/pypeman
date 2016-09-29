import asyncio

from pypeman.helpers import lazyload

all = []

class BaseEndpoint:

    def __init__(self):
        all.append(self)

    @asyncio.coroutine
    def start(self):
        pass


HTTPEndpoint = lazyload.load(__name__, 'pypeman.contrib.http', 'HTTPEndpoint', ['aiohttp'])
MLLPEndpoint = lazyload.load(__name__, 'pypeman.contrib.hl7', 'MLLPEndpoint', ['hl7'])

