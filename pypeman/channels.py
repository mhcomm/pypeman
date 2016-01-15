import asyncio
import datetime
import os
import uuid
import logging

from aiocron import crontab
from aiohttp import web
import aioftp

from pypeman import endpoints, message

all = []


class Dropped(Exception):
    pass


class Break(Exception):
    pass

class BaseChannel:

    dependencies = [] # List of module requirements


    def __init__(self, name=None):
        self.uuid = uuid.uuid4()
        all.append(self)
        self._nodes = []
        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__ + str(len(all))
        self.logger = logging.getLogger(self.name)

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in requirements """
        pass

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

    def graph(self, prefix=''):
        for node in self._nodes:
            if isinstance(node, SubChannel):
                print(prefix + '|—\\')
                node.graph(prefix= '|  ' + prefix)
            elif isinstance(node, ConditionSubChannel):
                print(prefix + '|?\\')
                node.graph(prefix='|  ' + prefix)
                print(prefix + '|  -> Out')
            else:
                print(prefix + '|-' + node.__class__.__name__)


class SubChannel(BaseChannel):
    """ Subchannel used for fork """
    pass


class ConditionSubChannel(BaseChannel):
    """ ConditionSubchannel used for make alternative path """
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


class FileWatcherChannel(BaseChannel):
    def __init__(self, path='', regex='*', interval=1):
        super().__init__()
        self.path = path
        self.regex = regex
        self.interval = interval
        self.found = os.path.exists(path)
        self.mtime = os.stat(path).st_mtime if self.found else None
        self.loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def start(self):
        asyncio.async(self.watch_for_file())

    def watch_for_file(self):
        # TODO watch multiple files
        # TODO Use pyinotify see -> https://pypi.python.org/pypi/butter

        if (not self.found and os.path.exists(self.path) or
            self.found and os.stat(self.path).st_mtime > self.mtime):
            self.found = True
            self.mtime = os.stat(self.path).st_mtime
            with open(self.path) as file:
                msg = message.Message()
                msg.payload = file.read()
                yield from self.process(msg)

        yield from asyncio.sleep(self.interval)
        asyncio.async(self.watch_for_file())


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
