import json
import asyncio
from concurrent.futures import ThreadPoolExecutor

from pypeman.message import Message
from pypeman.channels import Dropped, Break

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
    def process(self, msg):
        print(self.channel.uuid, msg.payload)
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


