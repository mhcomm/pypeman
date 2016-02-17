import os
import json
import asyncio
import logging

from datetime import datetime

from urllib import parse

from concurrent.futures import ThreadPoolExecutor

from pypeman.message import Message
from pypeman.channels import Dropped, Break

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()

# All declared nodes register here
all = []

# used to share external dependencies
ext = {}


class BaseNode:
    """ Base of all Node """
    dependencies = []

    def __init__(self, *args, **kwargs):
        self.channel = None
        all.append(self)

        self.name = kwargs.pop('name',self.__class__.__name__ + "_" + str(len(all)))

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in dependencies """
        pass

    @asyncio.coroutine
    def handle(self, msg):
        result = yield from asyncio.coroutine(self.process)(msg)
        return result

    def process(self, msg):
        return msg


class RaiseError(BaseNode):
    def process(self, msg):
        raise Exception("Test node")


class DropNode(BaseNode):
    def process(self, msg):
        raise Dropped()


class BreakNode(BaseNode):
    def process(self, msg):
        raise Break()


class Empty(BaseNode):
    def process(self, msg):
        return Message()


class ThreadNode(BaseNode):
    # TODO create class ThreadPool ?

    @asyncio.coroutine
    def handle(self, msg):
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = yield from loop.run_in_executor(executor, self.process, msg)
            return result


class Log(BaseNode):
    def __init__(self, *args, **kwargs):
        self.lvl = kwargs.pop('level', logging.DEBUG)
        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.channel.logger.log(self.lvl, 'Uid channel: %r', self.channel.uuid)
        self.channel.logger.log(self.lvl, 'Uid message: %r', msg.uuid)
        self.channel.logger.log(self.lvl, 'Payload: %r', msg.payload)
        return msg


class JsonToPython(BaseNode):
    def process(self, msg):
        msg.payload = json.loads(msg.payload)
        msg.content_type = 'application/python'
        return msg


class PythonToJson(BaseNode):
    def process(self, msg):
        msg.payload = json.dumps(msg.payload)
        msg.content_type = 'application/json'
        return msg


class XMLToPython(BaseNode):
    dependencies = ['xmltodict']

    def __init__(self, *args, **kwargs):
        self.process_namespaces = kwargs.pop('process_namespaces', False)
        super().__init__(*args, **kwargs)

    def import_modules(self):
        if 'xmltodict' not in ext:
            import xmltodict
            ext['xmltodict'] = xmltodict

    def process(self, msg):
        msg.payload = ext['xmltodict'].parse(msg.payload, process_namespaces=self.process_namespaces)
        msg.content_type = 'application/python'
        return msg


class PythonToXML(BaseNode):
    dependencies = ['xmltodict']

    def __init__(self, *args, **kwargs):
        self.pretty = kwargs.pop('pretty', False)
        super().__init__(*args, **kwargs)

    def import_modules(self):
        if 'xmltodict' not in ext:
            import xmltodict
            ext['xmltodict'] = xmltodict

    def process(self, msg):
        msg.payload = ext['xmltodict'].unparse(msg.payload, pretty=self.pretty)
        msg.content_type = 'application/xml'
        return msg


class Encode(BaseNode):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = msg.payload.encode(self.encoding)
        return msg


class Decode(BaseNode):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = msg.payload.decode(self.encoding)
        return msg


class FileWriter(ThreadNode):
    def __init__(self, *args, **kwargs):
        self.path = kwargs.pop('path')
        self.binary_mode = kwargs.pop('binary_mode', False)
        super().__init__(*args, **kwargs)

    def process(self, msg):
        with open(self.path, 'w' + ('b' if self.binary_mode else '')) as file:
            file.write(msg.payload)
        return msg


# TODO put stores in specific file ?
class NullStoreBackend():
    """ For testing purpose """
    def store(self, message):
        pass


class FileStoreBackend():
    def __init__(self, path, filename, channel):
        self.path = path
        self.filename = filename
        self.counter = 0
        self.channel = channel

    def store(self, message):
        today = datetime.now()

        context = {'counter':self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day,
                   'hour': today.hour,
                   'second': today.second,
                   'muid': message.uuid,
                   'cuid': self.channel.uuid
                   }

        filepath = os.path.join(self.path, self.filename % context)

        try:
            # Make missing dir if any
            os.makedirs(os.path.dirname(filepath))
        except FileExistsError:
            pass

        with open(filepath, 'w') as file_:
            file_.write(message.payload)

        self.counter += 1


class MessageStore(ThreadNode):
    def __init__(self, *args, **kwargs):

        self.uri = kwargs.pop('uri')
        parsed = parse.urlparse(self.uri)
        print(parsed)

        super().__init__(*args, **kwargs)

        if parsed.scheme == 'file':
            filename = parsed.query.split('=')[1]

            self.backend = FileStoreBackend(path=parsed.path, filename=filename, channel=self.channel)
        else:
            self.backend = NullStoreBackend()


    def process(self, msg):
        self.backend.store(msg)
        return msg


class HL7ToPython(BaseNode):
    dependencies = ['hl7']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def import_modules(self):
        if 'hl7' not in ext:
            import hl7
            ext['hl7'] = hl7

    def process(self, msg):
        msg.payload = ext['hl7'].parse(msg.payload)
        msg.content_type = 'application/python'
        return msg


class PythonToHL7(BaseNode):
    dependencies = ['hl7']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def import_modules(self):
        if 'hl7' not in ext:
            import hl7
            ext['hl7'] = hl7

    def process(self, msg):
        msg.payload = str(msg.payload)
        msg.content_type = 'text/hl7'
        return msg


class SaveFile(BaseNode):
    def __init__(self, filename=None, path=None, *args, **kwargs):
        self.filename = filename
        self.path = path
        self.counter = 0
        super().__init__(*args, **kwargs)

    def process(self, msg):

        if self.filename:
            name = self.filename
        else:
            name = msg.meta['filename']

        if self.path:
            path = self.path
        else:
            path = msg.meta['filepath']

        today = datetime.now()

        context = {'counter': self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day,
                   'hour': today.hour,
                   'second': today.second,
                   }

        dest = os.path.join(path, name % context)

        with open(dest, 'w') as file_:
            file_.write(msg.payload)

        self.counter += 1

        return msg


class MappingNode(BaseNode):
    def __init__(self, *args, **kwargs):
        self.mapping = kwargs.pop('mapping')
        self.recopy = kwargs.pop('recopy')
        path = kwargs.pop('path', None)

        self.path = 'payload'
        if path:
            self.path += '.' + path

        super().__init__(*args, **kwargs)

    def process(self, msg):
        current = msg
        parts = self.path.split('.')
        for part in parts:
            try:
                current = current[part]
            except (TypeError, KeyError):
                current = getattr(current, part)

        old_dict = current
        new_dict = {}

        for mapItem in self.mapping:
            mapItem.conv(old_dict, new_dict, msg)
        if self.recopy:
            new_dict.update(old_dict)

        dest = msg
        for part in parts[:-1]:
            try:
                dest = dest[part]
            except (TypeError, KeyError):
                dest = getattr(dest, part)

        try:
            dest[parts[-1]] = new_dict
        except KeyError:
            setattr(dest, parts[-1], new_dict)

        return msg
