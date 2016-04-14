import asyncio
import datetime
import os
import uuid
import logging
import re
import types


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
    STARTING, RUNNING, STOPPING, STOPPED  = range(4)

    dependencies = [] # List of module requirements

    def __init__(self, name=None, parent_channel=None):
        self.uuid = uuid.uuid4()

        all.append(self)
        self._nodes = []
        self.status = None

        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__ + "_" + str(len(all))

        self.logger = logging.getLogger(self.name)

        if parent_channel:
            self.parent_uids = [parent_channel.uuid]
            self.parent_names = [parent_channel.name]
            if parent_channel.parent_uids:
                self.parent_uids.append(parent_channel.parent_uids)
                self.parent_names.append(parent_channel.parent_names)
        else:
            self.parent_uids = None

        self.next_node = None

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in dependencies """
        pass

    @asyncio.coroutine
    def start(self):
        """ Start the channel """
        self.init_node_graph()

    def init_node_graph(self):
        previous_node = self._nodes[0]

        for node in self._nodes[1:]:
            previous_node.next_node = node
            previous_node = node

    @asyncio.coroutine
    def stop(self):
        """ Stop the channel """
        self.status = BaseChannel.STOPPING

    def add(self, *args):
        for node in args:
            node.channel = self
            self._nodes.append(node)
        return self

    def fork(self):
        s = SubChannel(parent_channel=self)
        self._nodes.append(s)
        return s

    def when(self, condition):
        s = ConditionSubChannel(condition, parent_channel=self)
        self._nodes.append(s)
        return s

    @asyncio.coroutine
    def handle(self, msg):
        result = yield from self.process(msg)

        if self.next_node:

            if isinstance(result, types.GeneratorType):
                for res in result:
                    result = yield from self.next_node.handle(res)
                    # TODO Here result is last value returned. Is it a good idea ?
            else:
                result = yield from self.next_node.handle(result)

        return result

    @asyncio.coroutine
    def process(self, msg):
        res = yield from self._nodes[0].handle(msg)
        return res

    def graph(self, prefix='', dot=False):
        for node in self._nodes:
            if isinstance(node, SubChannel):
                print(prefix + '|—\\')
                node.graph(prefix= '|  ' + prefix)
            elif isinstance(node, ConditionSubChannel):
                print(prefix + '|?\\')
                node.graph(prefix='|  ' + prefix)
                print(prefix + '|  -> Out')
            else:
                print(prefix + '|-' + node.name)

    def graph_dot(self, previous='', end=''):
        after = []
        for node in self._nodes:
            if isinstance(node, SubChannel):
                after.append((previous, '', node))
            elif isinstance(node, ConditionSubChannel):
                after.append((previous, end, node))
            else:
                print('->' + node.name, end='')
                previous = node.name
        if end:
            print("->" + end + ";")
        else:
            print(";")

        for prev, end, sub in after:
            print(prev, end='')
            sub.graph_dot(previous=prev, end=end)


class SubChannel(BaseChannel):
    """ Subchannel used for fork """

    @asyncio.coroutine
    def process(self, msg):
        asyncio.async(self._nodes[0].handle(msg.copy()))
        return msg


class ConditionSubChannel(BaseChannel):
    """ ConditionSubchannel used for make alternative path but join at the end """

    def __init__(self, condition, **kwargs):
        super().__init__(**kwargs)
        self.condition = condition

    def test_condition(self, msg):
        if callable(self.condition):
            return self.condition(msg)
        return True


    # Keep this one to handle Condition sub channel as before
    # TODO make a casesubchannel
    @asyncio.coroutine
    def handle(self, msg):
        if self.test_condition(msg):
            result = yield from self.process(msg)
        else:
            if self.next_node:
                result = yield from self.next_node.handle(msg)
            else:
                result = msg

        return result

    # Uncomment me to handle new way of condition subchannel
    '''@asyncio.coroutine
    def process(self, msg):
        if self.test_condition(msg):
            result = yield from self._nodes[0].handle(msg)
            return result

        return msg'''


class HttpChannel(BaseChannel):
    dependencies = ['aiohttp']
    app = None

    def __init__(self, *args, endpoint=None, method='*', url='/', encoding=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.method = method
        self.url = url
        self.encoding = encoding
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.http_endpoint = endpoint

    def import_modules(self):
        if 'aiohttp_web' not in ext:
            from aiohttp import web
            ext['aiohttp_web'] = web

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        self.http_endpoint.add_route(self.method, self.url, self.handle)

    @asyncio.coroutine
    def handle(self, request):
        content = yield from request.text()
        msg = message.Message(content_type='http_request', payload=content, meta={'method': request.method})
        try:
            result = yield from self.process(msg)
            encoding = self.encoding or 'utf-8'
            return ext['aiohttp_web'].Response(body=result.payload.encode(encoding), status=result.meta.get('status', 200))

        except Dropped:
            return ext['aiohttp_web'].Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as e:
            return ext['aiohttp_web'].Response(body=str(e).encode('utf-8'), status=503)


class FileWatcherChannel(BaseChannel):
    NEW, UNCHANGED, MODIFIED, DELETED  = range(4)

    def __init__(self, *args, path='', regex='.*', interval=1, binary_file=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.regex = regex
        self.interval = interval
        self.loop = asyncio.get_event_loop()
        self.dirflag = os.path.isdir(self.path)
        self.data = {}
        self.re = re.compile(self.regex)
        self.binary_file = binary_file

        # Set mtime for all existing matching files
        if os.path.exists(self.path):
            for filename in os.listdir(self.path):
                if self.re.match(filename):
                    filepath = os.path.join(self.path, filename)
                    mtime = os.stat(filepath).st_mtime
                    self.data[filename] = mtime
        else:
            self.logger.warning('path not exist: %r', self.path)

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        asyncio.async(self.watch_for_file())

    def file_status(self, filename):
        if filename in self.data:
            old_mtime = self.data[filename]
            filepath = os.path.join(self.path, filename)
            new_mtime = os.stat(filepath).st_mtime
            if new_mtime == old_mtime:
                return FileWatcherChannel.UNCHANGED
            elif new_mtime > old_mtime:
                return FileWatcherChannel.MODIFIED
        else:
            return FileWatcherChannel.NEW

    def watch_for_file(self):
        yield from asyncio.sleep(self.interval)
        try:
            if os.path.exists(self.path):
                listfile = os.listdir(self.path)
                listfile.sort()

                for filename in listfile:
                    if self.re.match(filename):
                        status = self.file_status(filename)
                        # TODO watch deleted files ?
                        if status in [FileWatcherChannel.MODIFIED, FileWatcherChannel.NEW]:
                            filepath = os.path.join(self.path, filename)
                            self.data[filename] =  os.stat(filepath).st_mtime

                            # Read file and make message
                            if self.binary_file:
                                mode = "rb"
                            else:
                                mode = "r"
                            with open(filepath, mode) as file:
                                msg = message.Message()
                                msg.payload = file.read()
                                msg.meta['filename'] = filename
                                msg.meta['filepath'] = filepath
                                yield from self.process(msg)
                                # TODO add option to handle message async (next line)
                                # asyncio.async(self.process(msg))

        finally:
            if not self.status in (BaseChannel.STOPPING, BaseChannel.STOPPED,):
                asyncio.async(self.watch_for_file())


class TimeChannel(BaseChannel):
    dependencies = ['aiocron']

    def __init__(self, *args, cron='', **kwargs):
        super().__init__(*args, **kwargs)
        self.cron = cron

    def import_modules(self):
        if 'aiocron_crontab' not in ext:
            from aiocron import crontab

            ext['aiocron_crontab'] = crontab

    @asyncio.coroutine
    def start(self):
        super().start()
        ext['aiocron_crontab'](self.cron, func=self.tic, start=True)

    @asyncio.coroutine
    def tic(self):
        msg = message.Message()
        msg.payload = datetime.datetime.now()
        yield from self.handle(msg)


    @asyncio.coroutine
    def handle(self, msg):
        result = yield from self.process(msg)
        return result


class MLLPChannel(BaseChannel):
    dependencies = ['hl7']

    def __init__(self, *args, endpoint=None, **kwargs):
        super().__init__(*args, **kwargs)
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.mllp_endpoint = endpoint

    def import_modules(self):
        if 'hl7' not in ext:
            import hl7
            ext['hl7'] = hl7

    @asyncio.coroutine
    def start(self):
        yield from super().start()
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
