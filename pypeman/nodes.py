import os
import json
import asyncio
import logging

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
    def __init__(self, path, filename):
        self.path = path
        self.filename = filename
        self.counter = 0

    def store(self, message):
        from datetime import date

        today = date.today()

        context = {'counter':self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day
                   }

        filename = os.path.join(self.path, self.filename % context)
        print(filename)

        with open(filename, 'wb') as file:
            file.write(message.payload)

        self.counter += 1


class MessageStore(ThreadNode):
    def __init__(self, *args, **kwargs):

        self.uri = kwargs.pop('uri')
        parsed = parse.urlparse(self.uri)
        print(parsed)

        if parsed.scheme == 'file':
            filename = parsed.query.split('=')[1]

            self.backend = FileStoreBackend(path=parsed.path, filename=filename)
        else:
            self.backend = NullStoreBackend()

        super().__init__(*args, **kwargs)

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
    def __init__(self, fileName=None, filePath=None, *args, **kwargs):
        self.fileName = fileName
        self.filePath = filePath
        super().__init__(*args, **kwargs)

    def process(self, msg):
        if self.fileName:
            name = self.fileName
        else:
            name = msg.meta['fileName']
        if self.filePath:
            path = self.filePath
        else:
            path = msg.meta['filePath']
        sav = os.path.join(path, name)
        with open(sav, 'w') as file_:
            file_.write(msg.payload)
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
            newDict.update(old_dict)

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
