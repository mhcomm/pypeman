import asyncio
import datetime


from aiocron import crontab
from aiohttp import web

import endpoints

all = []

class Channel:
    def __init__(self):
        all.append(self)
        self._nodes = []

    def add(self, *args):
        for node in args:
            self._nodes.append(node)

    @asyncio.coroutine
    def process(self, message):
        # TODO Save message here at start
        result = message
        for node in self._nodes:
            result = yield from node.handle(result)

        return result


class HttpChannel(Channel):
    app = None

    def __init__(self, method='*', url='/'):
        super().__init__()
        self.method = method
        self.url = url

    @asyncio.coroutine
    def start(self):
        endpoints.http_endpoint.add_route(self.method, self.url, self.handle)

    @asyncio.coroutine
    def handle(self, request):
        content = yield from request.text()
        text = yield from self.process(content)
        return web.Response(body=text.encode('utf-8'))


class TimeChannel(Channel):

    def __init__(self, cron=''):
        super().__init__()
        self.cron = cron

    @asyncio.coroutine
    def start(self):
        crontab(self.cron, func=self.handle, start=True)

    @asyncio.coroutine
    def handle(self):
        result = yield from self.process(datetime.datetime.now())
        return result