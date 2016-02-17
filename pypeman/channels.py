import asyncio
import datetime
import os
import uuid
import logging
import re
from enum import Enum


from pypeman import endpoints, message

logger = logging.getLogger(__name__)

# List all channel registered
all = []

# used to share external dependencies
ext = {}


class Dropped(Exception):
    pass

class Rejected(Exception):
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
        """ Use this method to import specific external modules listed in dependencies """
        pass

    @asyncio.coroutine
    def start(self):
        """ Start the channel """
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
            return self.condition(msg)
        return True


class HttpChannel(BaseChannel):
    dependencies = ['aiohttp']
    app = None

    def __init__(self, endpoint=None, method='*', url='/'):
        super().__init__()
        self.method = method
        self.url = url
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.http_endpoint = endpoint

    def import_modules(self):
        if 'aiohttp_web' not in ext:
            from aiohttp import web

            ext['aiohttp_web'] = web

    @asyncio.coroutine
    def start(self):
        self.http_endpoint.add_route(self.method, self.url, self.handle)

    @asyncio.coroutine
    def handle(self, request):
        content = yield from request.text()
        msg = message.Message(content_type='http_request', payload=content, meta={'method': request.method})
        try:
            result = yield from self.process(msg)
        except Dropped:
            return ext['aiohttp_web'].Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as e:
            return ext['aiohttp_web'].Response(body=str(e).encode('utf-8'), status=503)

        return ext['aiohttp_web'].Response(body=result.payload.encode('utf-8'), status=result.meta.get('status', 200))

class State(Enum):
    MISSING, NEW, DEL, MODIFIED, UNCHANGE = range(5)

class FileWatcherChannel(BaseChannel):

    def __init__(self, path='', regex='*', interval=1):
        super().__init__()
        self.path = path
        self.regex = regex
        self.interval = interval
        self.found = os.path.exists(path)
        self.mtime = os.stat(path).st_mtime if self.found else None
        self.loop = asyncio.get_event_loop()
        self.dirflag = os.path.isdir(self.path)
        self.data = {}
        if self.dirflag:
            for filename in os.listdir(self.path):
                reg = re.compile(self.regex)
                if reg.match(filename):
                    filepath = os.path.join(self.path, filename)
                    mtime = os.stat(filepath).st_mtime
                    self.data[filename] = mtime
        else:
            if self.found:
                self.data[self.path] = mtime

    @asyncio.coroutine
    def start(self):
        asyncio.async(self.watch_for_file())

    def state_path(self, filepath):
        if os.path.exists(filepath):
            new_time = os.stat(filepath).st_mtime
        else:
            new_time = None
        old_time = self.data.get(filepath)

        if not new_time and not old_time:
            logger.debug('State.MISSING: %r', State.MISSING)
            return State.MISSING
        elif new_time == old_time:
            logger.debug('State.UNCHANGE: %r', State.UNCHANGE)
            return State.UNCHANGE
        elif new_time and not old_time:
            logger.debug('State.NEW: %r', State.NEW)
            return State.NEW
        elif not new_time and old_time:
            logger.debug('State.DEL: %r', State.DEL)
            return State.DEL
        elif new_time > old_time:
            logger.debug('State.MODIFIED: %r', State.MODIFIED)
            return State.MODIFIED

    def watch_for_file(self):
        # TODO watch multiple files
        # TODO Use pyinotify see -> https://pypi.python.org/pypi/butter

        if not self.dirflag:
            statefile = self.state_path(self.path)
            if statefile == State.NEW or statefile == State.MODIFIED:
                with open(self.path) as file:
                    msg = message.Message()
                    msg.payload = file.read()
                    yield from self.process(msg)
            elif statefile == State.DEL:
                self.data.pop(filename)
        else:
            listfile = os.listdir(self.path)
            listfile.sort()
            for filename in listfile:
                reg = re.compile(self.regex)
                if reg.match(filename):
                    filepath = os.path.join(self.path, filename)
                    statefile = self.state_path(filepath)
                    logger.debug('statefile: %r', statefile)
                    if statefile == State.NEW or statefile == State.MODIFIED:
                        logger.debug('filename: %r', filename)
                        mtime = os.stat(filepath).st_mtime
                        self.data[filename] = mtime
                        with open(filepath) as file:
                            msg = message.Message()
                            msg.payload = file.read()
                            yield from self.process(msg)

            for key in self.data.keys():
                filepath = os.path.join(self.path, key)
                statefile = self.state_path(filepath)
                if statefile == State.DEL:
                    self.data.pop(key)

        #
        # if (not self.found and os.path.exists(self.path) or
        #     self.found and os.stat(self.path).st_mtime > self.mtime):
        #     self.found = True
        #     self.mtime = os.stat(self.path).st_mtime
        #     if os.path.isfile(self.path):
        #         with open(self.path) as file:
        #             msg = message.Message()
        #             msg.payload = file.read()
        #             yield from self.process(msg)
        #     else:
        #         for filename in os.listdir(self.path):
        #             reg = re.compile(self.regex)
        #             reg.match(filename)
        #             if()

        yield from asyncio.sleep(self.interval)
        asyncio.async(self.watch_for_file())


class TimeChannel(BaseChannel):
    dependencies = ['aiocron']

    def __init__(self, cron=''):
        super().__init__()
        self.cron = cron

    def import_modules(self):
        if 'aiocron_crontab' not in ext:
            from aiocron import crontab

            ext['aiocron_crontab'] = crontab

    @asyncio.coroutine
    def start(self):
        ext['aiocron_crontab'](self.cron, func=self.handle, start=True)

    @asyncio.coroutine
    def handle(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        result = yield from self.process(msg)
        return result


class MLLPChannel(BaseChannel):
    dependencies = ['hl7']

    def __init__(self, endpoint=None):
        super().__init__()
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.mllp_endpoint = endpoint

    def import_modules(self):
        if 'hl7' not in ext:
            import hl7
            ext['hl7'] = hl7

    @asyncio.coroutine
    def start(self):
        self.mllp_endpoint.set_handler(handler=self.handle)

    @asyncio.coroutine
    def handle(self, hl7_message):
        content = hl7_message
        msg = message.Message(content_type='text/hl7', payload=content, meta={})
        try:
            result = yield from self.process(msg)
            return result.payload
        except Dropped:
            ack = ext['hl7'].parse(content)
            return ack.create_ack('AA')
        except Rejected:
            ack = ext['hl7'].parse(content)
            return str(ack.create_ack('AR'))
        except Exception:
            ack = ext['hl7'].parse(content)
            return str(ack.create_ack('AE'))
