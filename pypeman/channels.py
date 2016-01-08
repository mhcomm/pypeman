import asyncio
import datetime
import copy
import uuid

from aiocron import crontab
from aiohttp import web

from pypeman import endpoints, message

all = []


class Dropped(Exception):
    pass


class Break(Exception):
    pass

class BaseChannel:
    def __init__(self):
        self.uuid = uuid.uuid4()
        all.append(self)
        self._nodes = []

    @asyncio.coroutine
    def start(self):
        pass

    def add(self, *args):
        for node in args:
            node.channel = self
            self._nodes.append(node)
        return self

    def fork(self):
        s = SubChannel()
        self._nodes.append(s)
        return s

    def when(self, condition):
        s = ConditionSubChannel(condition)
        self._nodes.append(s)
        return s


    '''def join(self, node):
        self._nodes.append(node.new_input())'''

    @asyncio.coroutine
    def process(self, message):
        # TODO Save message here at start
        result = message

        for node in self._nodes:
            if isinstance(node, SubChannel):
                asyncio.async(node.process(result.copy()))
            elif isinstance(node, ConditionSubChannel):
                if node.test_condition(result):
                    result = yield from node.process(result)
                    return result
            else:
                try:
                    result = yield from node.handle(result)
                except Break:
                    break

        return result


class SubChannel(BaseChannel):
    """ Subchannel used for fork
    """
    pass


class ConditionSubChannel(BaseChannel):
    """ Subchannel used for fork
    """
    def __init__(self, condition):
        super().__init__()
        self.condition = condition

    def test_condition(self, msg):
        if callable(self.condition):
            self.condition(msg)
        return True


class HttpChannel(BaseChannel):
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
        msg = message.Message(content_type='http_request', payload=content, meta={'method': request.method})
        try:
            result = yield from self.process(msg)
        except Dropped:
            return web.Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as e:
            return web.Response(body=str(e).encode('utf-8'), status=503)

        return web.Response(body=result.payload.encode('utf-8'), status=result.meta.get('status', 200))


class TimeChannel(BaseChannel):
    def __init__(self, cron=''):
        super().__init__()
        self.cron = cron

    @asyncio.coroutine
    def start(self):
        crontab(self.cron, func=self.handle, start=True)

    @asyncio.coroutine
    def handle(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        result = yield from self.process(msg)
        return result


class MLLPChannel(BaseChannel):
    pass
