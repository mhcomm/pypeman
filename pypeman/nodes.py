import os
import json
import types
import asyncio
import logging

from datetime import datetime
from collections import OrderedDict

from urllib import parse

from concurrent.futures import ThreadPoolExecutor

from pypeman.message import Message
from pypeman.channels import Dropped, Break

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()

from copy import deepcopy

# All declared nodes register here
all = []

# used to share external dependencies
ext = {}


class BaseNode:
    """ Base of all Nodes.
    If you create a new node, you must inherit from this class and implement `process` method.
    """
    dependencies = []

    def __init__(self, *args, **kwargs):
        self.channel = None
        all.append(self)
        self.name = kwargs.pop('name',self.__class__.__name__ + "_" + str(len(all)))
        self.store_output_as = kwargs.pop('store_output_as', None)
        self.store_input_as = kwargs.pop('store_input_as', None)
        self.passthrough = kwargs.pop('passthrough', None)
        self.next_node = None

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in dependencies """
        pass

    @asyncio.coroutine
    def handle(self, msg):
        """ Handle message is called by channel to launch process method on it.
        Some other structural processing take place here.
        Please, don't modify unless you know what you are doing.

        :param msg: incoming message
        :return: modified message after a process call and some treatment
        """

        # TODO : Make sure exceptions are well raised (does not happen if i.e 1/0 here atm)
        if self.store_input_as:
            msg.ctx[self.store_input_as] = dict(
                meta=dict(msg.meta),
                payload=deepcopy(msg.payload),
            )

        if self.passthrough:
            old_msg = msg.copy()

        result = self.run(msg)

        if isinstance(result, asyncio.Future):
            result = yield from result


        if self.next_node:
            if isinstance(result, types.GeneratorType):
                for res in result:
                    result = yield from self.next_node.handle(res)
                    # TODO Here result is last value returned. Is it a good idea ?
            else:
                if self.store_output_as:
                    result.ctx[self.store_output_as] = dict(
                        meta=dict(result.meta),
                        payload=deepcopy(result.payload),
                    )

                if self.passthrough:
                    result.payload = old_msg.payload
                    result.meta = old_msg.meta

                result = yield from self.next_node.handle(result)

        return result

    def run(self, msg):
        """ Used to overload behaviour like thread Node without rewriting handle process """
        result = self.process(msg)
        return result

    def process(self, msg):
        """ Implement this function in child classes to create
        a new Node.
        :param msg: The incoming message
        :return: The processed message
        """
        return msg

    def __str__(self):
        return "<%s(%s)>" % (self.channel.name, self.name)

class RaiseError(BaseNode):
    def process(self, msg):
        raise Exception("Test node")


class DropNode(BaseNode):
    """ This node used to tell the channel the message is Dropped. """
    def process(self, msg):
        raise Dropped()


class BreakNode(BaseNode):
    """ This node used to tell the channel the message is ????. """
    def process(self, msg):
        raise Break()


class Empty(BaseNode):
    """ Return an empty new message. """
    def process(self, msg):
        return Message()


class SetCtx(BaseNode):
    """ Push the message in the context with the key `ctx_name` """

    def __init__(self, ctx_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ctx_name = ctx_name

    def process(self, msg):
        msg.meta = msg.ctx[self.ctx_name]['meta']
        msg.payload = msg.ctx[self.ctx_name]['payload']

        return msg

class ThreadNode(BaseNode):
    """ Inherit from this class instead of BaseNode to avoid
    long run node blocking main event loop.
    """
    # TODO create class ThreadPool or channel ThreadPool or Global ?

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(max_workers=3)

    def run(self, msg):
        result = self.channel.loop.run_in_executor(self.executor, self.process, msg)

        return result


class Log(BaseNode):
    """ Node to show some information about node, channel and message. Use for debug.
    """
    def __init__(self, *args, **kwargs):
        self.lvl = kwargs.pop('level', logging.INFO)
        self.show_ctx = kwargs.pop('show_ctx', None)
        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.channel.logger.log(self.lvl, '%s %s', repr(self), repr(msg))

        if self.channel.parent_uids:
            self.channel.logger.log(self.lvl, 'Parent channels: %r', repr(self.channel.parent_names))

        self.channel.logger.log(self.lvl, 'Payload: %r', repr(msg.payload))

        if self.show_ctx:
            self.channel.logger.log(self.lvl, 'Contexts: %r', [repr(ctx) for ctx in msg.ctx])

        return msg

class JsonToPython(BaseNode):
    """ Convert json message payload to python dict."""
    # TODO encoding management
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = json.loads(msg.payload, encoding=self.encoding)
        msg.content_type = 'application/python'
        return msg


class PythonToJson(BaseNode):
    """ Convert python payload to json."""
    def process(self, msg):
        msg.payload = json.dumps(msg.payload)
        msg.content_type = 'application/json'
        return msg


class XMLToPython(BaseNode):
    """ Convert XML message payload to python dict."""

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
    """ Convert python payload to XML."""
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
    """ Encode payload in specified encoding to byte.
    """
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = msg.payload.encode(self.encoding)
        return msg


class Decode(BaseNode):
    """ Decode payload from byte to specified encoding
    """
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf-8')
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = msg.payload.decode(self.encoding)
        return msg


# TODO put stores in specific file ?
class NullStoreBackend():
    """ For testing purpose """
    def store(self, message):
        pass


class FileStoreBackend():
    """ Backend used to store message with `MessageStore` node.
    """
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
                   'cuid': getattr(self.channel, 'uuid', '???')
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
    """ Store a message in specified store """
    def __init__(self, *args, **kwargs):

        self.uri = kwargs.pop('uri')
        parsed = parse.urlparse(self.uri)

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
    """ Convert hl7 payload to python struct."""

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
    """ Convert python payload to HL7."""

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


class FileReader(BaseNode):
    """ Reads a file and sets payload to the file's contents. """
    def __init__(self, filename=None, path=None, binary_file=False, *args, **kwargs):
        self.filename = filename
        self.path = path
        self.binary_file = binary_file
        self.counter = 0
        super().__init__(*args, **kwargs)

    def process(self, msg):
        if self.filename:
            if callable(self.filename):
                name = self.filename(msg)
            else:
                name = self.filename
        else:
            name = msg.meta['filename']

        if self.path:
            path = self.path
        else:
            path = os.path.dirname(msg.meta['filepath'])

        filepath = os.path.join(path, name)

        if self.binary_file:
            mode = "rb"
        else:
            mode = "r"
        with open(filepath, mode) as file:
            msg.payload = file.read()
            msg.meta['filename'] = name
            msg.meta['filepath'] = filepath

        self.counter += 1
        return msg


class FileWriter(BaseNode):
    """ Write a file with the message content. """
    def __init__(self, filename=None, path=None, binary_mode=False, safe_file=False, *args, **kwargs):
        self.filename = filename
        self.path = path
        self.binary_mode = binary_mode
        self.counter = 0
        self.safe_file = safe_file
        super().__init__(*args, **kwargs)

    def process(self, msg):

        if self.filename:
            name = self.filename
        else:
            name = msg.meta['filename']

        if self.path:
            path = self.path
        else:
            path = os.path.dirname(msg.meta['filepath'])

        today = datetime.now()

        context = {'counter': self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day,
                   'hour': today.hour,
                   'second': today.second,
                   }

        dest = os.path.join(path, name % context)

        old_file = dest
        if self.safe_file:
            dest = old_file + '.tmp'

        with open(dest, 'w' + ('b' if self.binary_mode else '')) as file_:
            file_.write(msg.payload)

        if self.safe_file:
            os.rename(dest, old_file)

        self.counter += 1

        return msg


class MappingNode(BaseNode):
    """ Used to map input message keys->values to another keys->values """

    def __init__(self, *args, **kwargs):
        self.mapping = kwargs.pop('mapping')
        self.recopy = kwargs.pop('recopy')
        path = kwargs.pop('path', "")

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


class RequestNode(ThreadNode):
    """ Http request node """
    dependencies = ['requests']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = kwargs.pop('url')
        self.content_type = kwargs.pop('content_type', None)
        self.auth = kwargs.pop('auth', None)
        self.verify = kwargs.pop('verify', False)
        self.url = self.url.replace('%(meta.', '%(')
        self.payload_in_url_dict = 'payload.' in self.url

        # TODO: create used payload keys for better perf of generate_request_url()

    def import_modules(self):
        """ import modules """
        if 'requests' not in ext:
            import requests
            ext['requests'] = requests

    def generate_request_url(self, msg):

        url_dict = msg.meta
        if self.payload_in_url_dict:
            url_dict = dict(url_dict)
            try:
                for key, val in msg.payload.items():
                    url_dict['payload.' + key] = val
            except AttributeError:
                logger.exception("Payload must be a python dict if used to generate url. This can be fixed using JsonToPython node before your RequestNode")
                raise

        logger.debug("Completing url %r with data from %r" % (self.url, url_dict))
        return self.url % url_dict

    def handle_request(self, msg):
        """ generate url and handle request """
        url = self.generate_request_url(msg)
        logger.debug("Destination request url: %s", url)
        resp = ext['requests'].get(url=url, auth=self.auth, verify=self.verify)
        return str(resp.text)

    def process(self, msg):
        """ handles request """
        msg.payload = self.handle_request(msg)

        return msg


class ToOrderedDict(BaseNode):
    """ this node yields an ordered dict with the keys 'keys' and the values from the payload
       if the payload does not contain certain values defaults can be specified with defaults
    """
    NONE = object()
    def __init__(self, *args, **kwargs):
        self.keys = kwargs.pop('keys')
        defaults = kwargs.pop('defaults', dict())
        self.dflt_dict = OrderedDict()
        for key in self.keys:
            self.dflt_dict[key] = defaults.get(key, ToOrderedDict.NONE)
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
        new_dict = OrderedDict()

        for key in self.keys:
            val = old_dict.get(key, ToOrderedDict.NONE)
            if val is ToOrderedDict.NONE:
                val = self.dflt_dict[key]
            if val is not ToOrderedDict.NONE:
                new_dict[key] = val

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
