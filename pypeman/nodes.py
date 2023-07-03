import asyncio
import base64
import collections
import json
import logging
import os
import shutil
import smtplib
import types
import warnings

from datetime import datetime
from collections import OrderedDict
from fnmatch import fnmatch
from pathlib import Path

from email.mime.text import MIMEText


from urllib import parse

from concurrent.futures import ThreadPoolExecutor

from pypeman.message import Message
from pypeman.channels import Dropped
from pypeman.channels import Rejected
from pypeman.persistence import get_backend

logger = logging.getLogger(__name__)

# All declared nodes registered here
all_nodes = []
node_by_name = {}

# Can be redefined
default_thread_pool = ThreadPoolExecutor(max_workers=3)

SENTINEL = object()


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


def get_context(msg, date=None, counter=None):
    cdate = date or datetime.now()
    timestamp = msg.timestamp
    context = {'year': cdate.year,
               'month': cdate.month,
               'day': cdate.day,
               'hour': cdate.hour,
               'second': cdate.second,
               'msg_year': timestamp.year,
               'msg_month': timestamp.month,
               'msg_day': timestamp.day,
               'msg_hour': timestamp.hour,
               'msg_second': timestamp.second,
               'msg_uid': msg.uuid,
               'counter': counter
               }
    return context


class NodeException(Exception):
    """ custom exception """


def find_node(name="", match=None):
    """
    find nodes in a pypeman graph

    :param name:  name of node to search
                  name can contain * ? as specified in fnmatch.fnmatch
    """

    if name:
        return next(
            filter(lambda node: fnmatch(node.name, name), all_nodes),
            None,
            )
    return next(filter(match, all_nodes, None))


class BaseNode:
    """
    Base of all Nodes.
    If you create a new node, you must inherit from this class and
    implement `process` method.

    :func:`save_data <pypeman.nodes.BaseNode.save_data>`

    :param name: Name of node. Used in log or test.
    :param log_output: To enable output logging for this node.
    :store_output_as: Store output message in msg.ctx as specified key
    :store_input_as: Store input message in msg.ctx as specified key
    :passthrough: If True, node is executed but output message is same as input

    """

    _used_names = set()  # already used node names to ensure uniqueness

    def __init__(self, *args, name=None, log_output=False, **kwargs):
        cls = self.__class__
        self.channel = None

        all_nodes.append(self)

        name = name or cls.__name__ + "_" + str(len(all_nodes))
        if name in cls._used_names:
            raise NodeException(
                "can't create Node with name %s. It exists already" % name)
        cls._used_names.add(name)
        node_by_name[name] = self
        self.name = name

        self.store_output_as = kwargs.pop('store_output_as', None)
        self.store_input_as = kwargs.pop('store_input_as', None)
        self.passthrough = kwargs.pop('passthrough', None)
        self.next_node = None

        self.processed = 0

        if log_output:
            # Enable logging
            self._handle_without_log = self.handle
            setattr(self, 'handle', self._log_handle)

    def fullpath(self):
        """
        Return the channel name and node name dot concatened.
        """
        return "%s.%s" % (self.channel.name, self.name)

    async def handle(self, msg):
        """ Handle message is called by channel to launch process method on it.
        Some other structural processing take place here.
        Please, don't modify unless you know what you are doing.

        :param msg: incoming message
        :return: modified message after a process call and some treatment
        """

        # TODO : Make sure exceptions are well raised (does not happen if i.e 1/0 here atm)
        if self.store_input_as:
            msg.add_context(self.store_input_as, msg)
        if self.passthrough:
            old_msg = msg.copy()
        # Allow process as coroutine function
        if asyncio.iscoroutinefunction(self.process):
            result = await self.async_run(msg)
        else:
            result = self.run(msg)

        self.processed += 1

        if isinstance(result, asyncio.Future):
            result = await result

        self.channel.logger.debug(
            '%s node end handle msg %s, result is msg %s',
            str(self), str(msg), str(result))

        if self.next_node:
            if isinstance(result, types.GeneratorType):
                gene = result
                result = msg  # Necessary if all nodes result are dropped
                for res in gene:
                    try:
                        result = await self.next_node.handle(res)
                    except Dropped:
                        pass
                    # TODO Here result is last value returned. Is it a good idea ?
            else:
                if self.store_output_as:
                    msg.add_context(self.store_output_as, msg)

                if self.passthrough:
                    result.payload = old_msg.payload
                    result.meta = old_msg.meta

                result = await self.next_node.handle(result)

        return result

    async def _log_handle(self, msg):
        """
        Used when node logging is enabled. Log after node processing.
        """
        result = await self._handle_without_log(msg)

        # Log message
        result.log(logger=self.channel.logger, log_level=logging.DEBUG)

        return result

    async def _test_handle(self, msg):
        """ Specific handle for TEST mode to enable some testing and introspection operations
        like mock input and/or output, or count processed message.

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

        result = await self._handle(msg)

        return result

    async def async_run(self, msg):
        """ Used to overload behaviour like thread Node without rewriting handle process """
        result = await self.process(msg)
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

    async def save_data(self, key, value):
        """
        Save data in configured persistence backend for next usage.

        :param key: Key of saved data.
        :param value: Value saved.
        """
        await (await get_backend(self.channel.loop)).store(self.fullpath(), key, value)

    async def restore_data(self, key, default=SENTINEL):
        """
        Restore previously saved data from configured persistence backend.

        :param key: Key of restored data.
        :param default: if key is missing, don't raise exception and return this value instead.
        :return: Saved data if exist or default value if specified.
        """
        if default is not SENTINEL:
            return await (await get_backend(self.channel.loop)).get(self.fullpath(), key, default)
        else:
            return await (await get_backend(self.channel.loop)).get(self.fullpath(), key)

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
            setattr(self, 'handle', self._test_handle)

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
    """ Use this node to tell the channel the message is Dropped. """
    def __init__(self, message=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = message

    def process(self, msg):
        if self.message:
            raise Dropped(self.message)
        else:
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
        """
            Node specific params
            :param level: log level of node
            :param show_ctx: whether to log the context
        """
        self.lvl = kwargs.pop('level', logging.INFO)
        self.show_ctx = kwargs.pop('show_ctx', False)

        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.channel.logger.log(self.lvl, '%s %s', str(self), str(msg))

        if self.channel.parent_uids:
            self.channel.logger.log(self.lvl, 'Parent channels: %s', ', '.join(self.channel.parent_names))

        msg.log(logger=self.channel.logger, payload=True, meta=True,
                context=self.show_ctx, log_level=self.lvl)

        return msg


class Sleep(BaseNode):
    """ Wait `duration` seconds before returning message."""
    def __init__(self, *args, duration=1, **kwargs):
        self.duration = duration
        super().__init__(*args, **kwargs)

    async def process(self, msg):
        await asyncio.sleep(self.duration)
        return msg


class JsonToPython(BaseNode):
    """ Convert json message payload to python dict."""
    # TODO encoding management
    def __init__(self, *args, encoding='utf-8', **kwargs):
        self.encoding = encoding
        super().__init__(*args, **kwargs)

    def process(self, msg):
        encoded_payload = msg.payload.encode(self.encoding)
        msg.payload = json.loads(encoded_payload)
        msg.content_type = 'application/python'
        return msg


class PythonToJson(BaseNode):
    """ Convert python payload to json."""
    def __init__(self, *args, encoding='utf-8', indent=None, **kwargs):
        self.encoding = encoding
        super().__init__(*args, **kwargs)
        self.indent = indent

    def process(self, msg):
        msg.payload = json.dumps(msg.payload, indent=self.indent)
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

        context = {'counter': self.counter,
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
    def __init__(self, filename=None, filepath=None, binary_file=False, *args, **kwargs):
        self.filename = filename
        self.filepath = filepath
        self.binary_file = binary_file
        self.counter = 0
        if self.filename:
            warnings.warn("Filename deprecated, use filepath instead", DeprecationWarning)
        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.counter += 1
        if self.filepath:
            filepath = callable_or_value(self.filepath, msg)

        elif self.filename:
            filename = callable_or_value(self.filename, msg)

            path = os.path.dirname(msg.meta['filepath'])
            filepath = os.path.join(path, filename)
        else:
            filepath = msg.meta['filepath']

        context = get_context(msg=msg, counter=self.counter)
        filepath = filepath % context
        name = os.path.basename(filepath)

        if self.binary_file:
            mode = "rb"
        else:
            mode = "r"

        with open(filepath, mode) as file:
            msg.payload = file.read()
            msg.meta['filename'] = name
            msg.meta['filepath'] = filepath

        return msg


class FileWriter(BaseNode):
    """
        Write a file with the message content.
        Can create a validation file with no content but with same path and same
        base name with different extension (for example .ok)
    """
    def __init__(self, filepath=None, binary_mode=False, safe_file=True, create_valid_file=False,
                 validation_extension=".ok", *args, **kwargs):
        self.filepath = filepath
        self.binary_mode = binary_mode
        self.safe_file = safe_file
        self.create_valid_file = create_valid_file
        self.validation_extension = validation_extension
        self.first_filename = True
        self.counter = 0
        super().__init__(*args, **kwargs)

    def process(self, msg):
        self.counter += 1
        meta_filepath = msg.meta.get('filepath')

        if self.filepath:
            filepath = callable_or_value(self.filepath, msg)
        else:
            filepath = meta_filepath

        if not filepath:
            raise ValueError("filepath must be defined in parameters or in msg.meta")

        context = get_context(msg=msg, counter=self.counter)
        dest = filepath % context
        old_file = dest
        if self.safe_file:
            dest = old_file + '.tmp'
        with open(dest, 'w' + ('b' if self.binary_mode else '')) as file_:
            file_.write(msg.payload)
        if self.safe_file:
            os.rename(dest, old_file)
        if self.create_valid_file:
            validation_path = Path(old_file).with_suffix(self.validation_extension)
            validation_path.touch()
        return msg


class FileMover(BaseNode):
    """
    Move file

    Used to store files at another place
    params:
    dest_fpath : path of the destination folder (if it doesn't exists it creates it)
    """
    def __init__(self, dest_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dest_path = dest_path
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)

    def process(self, msg):
        dest_fpath = os.path.join(self.dest_path, msg.meta["filename"])
        logger.info("move file %s to dest %s", msg.meta["filepath"], dest_fpath)
        shutil.move(msg.meta["filepath"], dest_fpath)
        msg.meta["filepath"] = dest_fpath
        return msg


class FileCleaner(BaseNode):
    """
    Delete a file and/or all metafiles in a directory with same basename but
    with a given extension

    param:
    extensions_to_rm => list of all extensions to rm (example: [".ok"])
    msg.meta["filepath"]
    """
    def __init__(self, extensions_to_rm=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extensions_to_rm = extensions_to_rm

    def process(self, msg):
        fpath = Path(msg.meta['filepath'])
        if fpath.is_file():
            logger.info("delete %s ...", msg.meta["filepath"])
            fpath.unlink()
        if self.extensions_to_rm:
            for extension in self.extensions_to_rm:
                meta_path = fpath.with_suffix(extension)
                if meta_path.is_file():
                    logger.info("delete %s ...", (meta_path))
                    meta_path.unlink()


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


class YielderNode(BaseNode):
    """
    Take an iterable msg.payload and returns a generator
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process(self, msg):
        if not isinstance(msg.payload, collections.abc.Iterable):
            logger.error("Yielder node took a non iterable msg.payload: %r", msg.payload)
            raise Rejected()

        def generator(msg):
            payload = msg.payload
            ctx = msg.ctx
            meta = msg.meta
            for entry in payload:
                newmsg = Message(meta=meta, payload=entry)
                newmsg.ctx = ctx
                yield newmsg
        return generator(msg)


def reset_pypeman_nodes():
    """
    clears book keeping of all channels

    Can be useful for unit testing.
    """
    logger.info("clearing all_nodes and BaseNode._used-names.")
    all_nodes.clear()
    BaseNode._used_names.clear()

# Contrib nodes


# TODO: can we move this line to top of file?
from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.ctx', "CombineCtx", [])
wrap.add_lazy('pypeman.contrib.xml', "XMLToPython", ["xmltodict"])
wrap.add_lazy('pypeman.contrib.xml', "PythonToXML", ["xmltodict"])
wrap.add_lazy('pypeman.contrib.hl7', "HL7ToPython", ["hl7"])
wrap.add_lazy('pypeman.contrib.hl7', "PythonToHL7", ["hl7"])
wrap.add_lazy('pypeman.contrib.http', "HttpRequest", ["aiohttp"])
wrap.add_lazy('pypeman.contrib.http', "RequestNode", ["aiohttp"])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileWriter", [])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileReader", [])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileDeleter", [])
wrap.add_lazy('pypeman.contrib.ftp', "FTPFileDeleter", [])
wrap.add_lazy('pypeman.contrib.csv', "CSV2Python", [])
wrap.add_lazy('pypeman.contrib.csv', "CSVstr2Python", [])
wrap.add_lazy('pypeman.contrib.csv', "Python2CSVstr", [])
