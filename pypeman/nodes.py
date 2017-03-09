import os
import json
import types
import asyncio
import logging
import base64
import warnings

import smtplib
from email.mime.text import MIMEText

from datetime import datetime
from collections import OrderedDict

from urllib import parse

from concurrent.futures import ThreadPoolExecutor

from pypeman.message import Message
from pypeman.channels import Dropped

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()

from copy import deepcopy

# All declared nodes register here
all = []

# Can be redefined
default_thread_pool = ThreadPoolExecutor(max_workers=3)

def choose_first_not_none(*args):
    """ Choose first non None alternative in args.
    :param args: alternative list
    :return: the first non None alternative.
    """
    for a in args:
        if a is not None:
            return a
    return None


def callable_or_value(val, msg):
    """
    Return `val(msg)` if value is a callable else `val`.
    """
    if callable(val):
        name = val(msg)
    else:
        name = val

    return name


class BaseNode:
    """
    Base of all Nodes.
    If you create a new node, you must inherit from this class and
    implement `process` method.

    :param name: Name of node. Used in log or test.
    :param log_output: To enable output logging for this node.

    """

    def __init__(self, *args, name=None, log_output=False, **kwargs):
        self.channel = None
        all.append(self)

        self.name = name or self.__class__.__name__ + "_" + str(len(all))

        self.store_output_as = kwargs.pop('store_output_as', None)
        self.store_input_as = kwargs.pop('store_input_as', None)
        self.passthrough = kwargs.pop('passthrough', None)
        self.next_node = None

        self.processed = 0

        if log_output:
            # Enable logging
            self._handle_without_log = self.handle
            self.handle = self._log_handle

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

        # Allow process as coroutine function
        if asyncio.iscoroutinefunction(self.process):
            result = yield from self.async_run(msg)
        else:
            result = self.run(msg)

        self.processed += 1

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

    @asyncio.coroutine
    def _log_handle(self, msg):
        """
        Used when node logging is enabled. Log after node processing.
        """
        result = yield from self._handle_without_log(msg)

        self.channel.logger.info('%s node from handles %s', str(self), str(result))

        # Log message
        result.log(logger=self.channel.logger, log_level=logging.DEBUG)

        return result

    @asyncio.coroutine
    def _test_handle(self, msg):
        """ Specific handle for TEST mode to enable some testing and introspection operations like mock input and/or
        output, or count processed message.

        :param msg: Message to process.
        :return: Processed message.
        """
        # Keep last input
        self._last_input = msg.copy()

        if self._mock_input:
            if callable(self._mock_input):
                msg = self._mock_input(msg)
            else:
                msg = self._mock_input

        result = yield from self._handle(msg)

        return result

    @asyncio.coroutine
    def async_run(self, msg):
        """ Used to overload behaviour like thread Node without rewriting handle process """
        result = yield from self.process(msg)
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

    # Allow to mock input or
    def mock(self, input=None, output=None):
        """
        Allow to mock input or output of a node for testing purpose.

        :param input: A message to replace the input in this node.
        :param output: A return message to replace processing of this mock.
        """
        if input:
            self._mock_input = input

        if output:
            if not hasattr(self, '_orig_process'):
                self._orig_process = self.process

            def new_process(msg):
                if callable(output):
                    return output(msg)
                else:
                    return output

            self.process = new_process

    def _reset_test(self):
        """ Set test mode and reset test information """
        self.processed = 0

        if not hasattr(self, '_handle'):
            self._handle = self.handle
            self.handle = self._test_handle

        if hasattr(self, '_orig_process'):
            self.process = self._orig_process

        self._mock_input = None
        self._last_input = None

    def last_input(self):
        return self._last_input

    def __str__(self):
        return "<%s(%s)>" % (self.channel.name, self.name)


class RaiseError(BaseNode):
    def process(self, msg):
        raise Exception("Test node")


class Drop(BaseNode):
    """ This node used to tell the channel the message is Dropped. """
    def process(self, msg):
        raise Dropped()

class DropNode(Drop):
    def __init__(self, *args, **kwargs):
        warnings.warn("DropNode node is deprecated. Replace it by Drop node", DeprecationWarning)
        super().__init__(*args, **kwargs)


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
    """
    Inherit from this class instead of BaseNode to avoid
    long run node blocking main event loop.
    """

    def __init__(self, *args, thread_pool=None, **kwargs):
        super().__init__(*args, **kwargs)

        if thread_pool is None:
            self.executor = default_thread_pool
        else:
            self.executor = thread_pool

    def run(self, msg):
        result = self.channel.loop.run_in_executor(self.executor, self.process, msg)

        return result


class Log(BaseNode):
    """
    Node to show some information about node, channel and message. Use for debug.
    """
    def __init__(self, *args, **kwargs):
        self.lvl = kwargs.pop('level', logging.INFO)
        self.show_ctx = kwargs.pop('show_ctx', False)

        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.channel.logger.log(self.lvl, '%s %s', str(self), str(msg))

        if self.channel.parent_uids:
            self.channel.logger.log(self.lvl, 'Parent channels: %s', ', '.join(self.channel.parent_names))

        msg.log(logger=self.channel.logger, payload=True, meta=True, context=self.show_ctx, log_level=self.lvl)

        return msg


class Sleep(BaseNode):
    """ Wait `duration` seconds before returning message."""
    def __init__(self, *args, duration=1, **kwargs):
        self.duration = duration
        super().__init__(*args, **kwargs)

    @asyncio.coroutine
    def process(self, msg):
        yield from asyncio.sleep(self.duration, loop=self.channel.loop)
        return msg


class JsonToPython(BaseNode):
    """ Convert json message payload to python dict."""
    # TODO encoding management
    def __init__(self, *args, encoding='utf-8', **kwargs):
        self.encoding = encoding
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


class B64Encode(BaseNode):
    """ Encode payload in specified encoding to byte.
    """
    def __init__(self, *args, altchars=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.altchars = altchars

    def process(self, msg):
        msg.payload = base64.b64encode(msg.payload, altchars=self.altchars)
        return msg


class B64Decode(BaseNode):
    """ Decode payload from byte to specified encoding
    """
    def __init__(self, *args, altchars=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.altchars = altchars

    def process(self, msg):
        msg.payload = base64.b64decode(msg.payload, altchars=self.altchars)
        return msg


# TODO put Save in specific file ?
class SaveNullBackend():
    """ For testing purpose """
    def store(self, message):
        pass


class SaveFileBackend():
    """ Backend used to store message with ``Save`` node.
    """
    def __init__(self, path, filename, channel):
        self.path = path
        self.filename = filename
        self.counter = 0
        self.channel = channel

    def store(self, message):
        today = datetime.now()
        timestamp = message.timestamp

        context = {'counter':self.counter,
                   'year': today.year,
                   'month': today.month,
                   'day': today.day,
                   'hour': today.hour,
                   'second': today.second,
                   'msg_year': timestamp.year,
                   'msg_month': timestamp.month,
                   'msg_day': timestamp.day,
                   'msg_hour': timestamp.hour,
                   'msg_second': timestamp.second,
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


class Save(ThreadNode):
    """ Save a message in specified uri """
    def __init__(self, *args, uri=None, **kwargs):

        self.uri = uri
        parsed = parse.urlparse(self.uri)

        super().__init__(*args, **kwargs)

        if parsed.scheme == 'file':
            filename = parsed.query.split('=')[1]

            self.backend = SaveFileBackend(path=parsed.path, filename=filename, channel=self.channel)
        else:
            self.backend = SaveNullBackend()


    def process(self, msg):
        self.backend.store(msg)
        return msg


class MessageStore(Save):
    def __init__(self, *args, **kwargs):
        warnings.warn("MessageStore node is deprecated. Replace it by Save node", DeprecationWarning)
        super().__init__(*args, **kwargs)


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


class Map(BaseNode):
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


class MappingNode(Map):
    def __init__(self, *args, **kwargs):
        warnings.warn("MappingNode node is deprecated. Replace it by 'Map' node", DeprecationWarning)
        super().__init__(*args, **kwargs)


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

class Email(ThreadNode):
    """ Node that send Email.
    """
    def __init__(self, *args, host=None, port=None, user=None, password=None, ssl=False, start_tls=False,
                 subject=None, sender=None, recipients=None, content=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.subject = subject or ""
        self.sender = sender
        self.recipients = recipients
        self.content = content
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.start_tls = start_tls
        self.ssl = ssl

    def send_email(self, subject, sender, recipients, content):
        # TODO add crt arg

        msg = MIMEText(content)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)

        # Send the message via the configured smtp server.
        # TODO keep same connection during some time ?
        s = None
        if self.ssl:
            s = smtplib.SMTP_SSL(self.host, self.port)
        else:
            s = smtplib.SMTP(self.host, self.port)

        if self.user and self.password:
            s.login(self.user, self.password)

        if self.start_tls:
            s.starttls()

        s.sendmail(sender, recipients, msg.as_string())

        s.quit()

    def process(self, msg):
        content = choose_first_not_none(self.content, msg.payload)
        subject = choose_first_not_none(self.subject, msg.meta.get('subject'), 'No subject')
        sender = choose_first_not_none(self.sender, msg.meta.get('sender'), 'pypeman@example.com')
        recipients = choose_first_not_none(self.recipients, msg.meta.get('recipients'), [])

        if isinstance(recipients, str):
            recipients = [recipients]

        self.send_email(subject, sender, recipients, content)

        return msg


# Contrib nodes
from pypeman.helpers import lazyload

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.xml', "XMLToPython", ["xmltodict"])
wrap.add_lazy('pypeman.contrib.xml', "PythonToXML", ["xmltodict"])
wrap.add_lazy('pypeman.contrib.hl7', "HL7ToPython", ["hl7"])
wrap.add_lazy('pypeman.contrib.hl7', "PythonToHL7", ["hl7"])
wrap.add_lazy('pypeman.contrib.http', "HttpRequest", ["aiohttp"])
wrap.add_lazy('pypeman.contrib.http', "RequestNode", ["aiohttp"])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileWriter", [])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileReader", [])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileDeleter", [])


