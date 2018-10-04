import asyncio
import os
import uuid
import logging
import re
import types
import warnings

from asyncio import ensure_future

from pypeman import message, msgstore, events

logger = logging.getLogger(__name__)

# List all channel registered
all = []

_channels_names = set()


class Dropped(Exception):
    """ Used to stop process as message is processed. Default success should be returned.
    """
    pass


class Rejected(Exception):
    """ Used to tell caller the message is invalid with a error return.
    """
    pass


class ChannelStopped(Exception):
    """ The channel is stopped and can't process message.
    """
    pass


class BaseChannel:
    """
    Base channel are generic channels.
    If you want to create new channel, inherit from the base class and call
    ``self.handle(msg)`` method with generated message.

    :param name: Channel name is mandatory and must be unique through the whole project.
        Name gives a way to get channel in test mode.

    :param parent_channel: Used with sub channels. Don't specify yourself.

    :param loop: To specify a custom event loop.

    :param message_store_factory:     You can specify a message store (see below) at channel
        initialisation if you want to save all processed message. Use
        `message_store_factory` argument with  an instance of wanted message store factory.
    """
    STARTING, WAITING, PROCESSING, STOPPING, STOPPED = range(5)
    STATE_NAMES = ['STARTING', 'WAITING', 'PROCESSING', 'STOPPING', 'STOPPED']

    def __init__(self, name=None, parent_channel=None, loop=None, message_store_factory=None):

        self.uuid = uuid.uuid4()

        all.append(self)
        self._nodes = []
        self._node_map = {}
        self._status = BaseChannel.STOPPED
        self.processed_msgs = 0

        if name:
            self.name = name
        else:
            warnings.warn("Channels without names are deprecated", DeprecationWarning)
            self.name = self.__class__.__name__ + "_" + str(len(all))

        self.parent = None
        if parent_channel:
            # Use dot name hierarchy
            self.name = ".".join([parent_channel.name, self.name])

            self.parent = parent_channel

            # TODO parent channels usefull ?
            self.parent_uids = [parent_channel.uuid]
            self.parent_names = [parent_channel.name]
            if parent_channel.parent_uids:
                self.parent_uids.append(parent_channel.parent_uids)
                self.parent_names += parent_channel.parent_names
        else:
            self.parent_uids = None

        if self.name in _channels_names:
            raise NameError(
                "Duplicate channel name %r . "
                "Channel names must be unique !" % self.name
            )

        _channels_names.add(self.name)

        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

        self.logger = logging.getLogger('pypeman.channels.%s' % self.name)

        self.next_node = None

        self.message_store_factory = message_store_factory or msgstore.NullMessageStoreFactory()

        self.message_store = self.message_store_factory.get_store(self.name)

        self._first_start = True

        # Used to avoid multiple messages processing at same time
        self.lock = asyncio.Lock(loop=self.loop)

    @classmethod
    def status_id_to_str(cls, state_id):
        return cls.STATE_NAMES[state_id]

    @classmethod
    def status_str_to_id(cls, state):
        return cls.STATE_NAMES.index(state)

    @property
    def status(self):
        """ Getter for status """
        return self._status

    @status.setter
    def status(self, value):
        old_state = self._status
        self._status = value
        # Launch change state event
        ensure_future(events.channel_change_state.fire(channel=self, old_state=old_state, new_state=value),
                      loop=self.loop)

    def is_stopped(self):
        """
        :return: True if channel is in stopped or stopping state.
        """
        return self.status in (BaseChannel.STOPPING, BaseChannel.STOPPED,)

    async def start(self):
        """
        Start the channel. Called before starting processus. Can be overloaded to specify specific
        start procedure.
        """
        self.status = BaseChannel.STARTING
        if self._first_start:
            self.init_node_graph()
            self._first_start = False
        await self.message_store.start()
        self.status = BaseChannel.WAITING

    def init_node_graph(self):
        if self._nodes:
            previous_node = self._nodes[0]

            for node in self._nodes[1:]:
                previous_node.next_node = node
                previous_node = node

    async def stop(self):
        """
        Stop the channel. Called when pypeman shutdown.

        """
        self.status = BaseChannel.STOPPING
        # Verify that all messages are processed
        with (await self.lock):
            self.status = BaseChannel.STOPPED

    def _reset_test(self):
        """ Enable test mode and reset node data.
        """
        for node in self._nodes:
            node._reset_test()

    def add(self, *args):
        """
        Add specified nodes to channel (Shortcut for append).

        :param args: Nodes to add.
        """
        return self.append(*args)

    def _register_node(self, node):
        """
        Node registering to search.

        :param node: node to register.
        """
        self._node_map[node.name] = node
        if self.parent:
            self.parent._register_node(node)

    def get_node(self, name):
        """
        Return node with name in argument. Mainly used in tests.

        :param name: The searched node name.

        :return: Instance of Node or None if none found.
        """

        return self._node_map.get(name)

    def append(self, *args):
        """
        Append specified nodes to channel.

        :param args: Nodes to add.
        """
        for node in args:
            node.channel = self
            self._nodes.append(node)
            self._register_node(node)

        return self

    def fork(self, name=None, message_store_factory=None):
        """
        Create a new channel that process a copy of the message at this point.
        Subchannels are executed in parallel of main process.

        :return: The forked channel
        """

        s = SubChannel(
            name=name, parent_channel=self,
            message_store_factory=message_store_factory, loop=self.loop)
        self._nodes.append(s)
        return s

    def when(self, condition, name=None, message_store_factory=None):
        """
        New channel bifurcation that is executed only if condition is True. This channel
        replace further current channel processing.

        :param condition: Can be a value or a function that takes a message argument.

        :return: The conditional path channel.
        """

        s = ConditionSubChannel(
            condition=condition, name=name, parent_channel=self,
            message_store_factory=message_store_factory, loop=self.loop)
        self._nodes.append(s)
        return s

    def case(self, *conditions, names=None, message_store_factory=None):
        """
        Case between multiple conditions. For each condition specified, a
        channel is returned by this method in same order as condition are
        given.
        When processing a message, conditions are evaluated successively and
        the first returning true triggers the corresponding channel processing
        the message.
        When channel processing is finished, the next node is called.

        :param conditions: Multiple conditions, one for each returned channel.
            Should be a boolean or a function that takes a ``msg`` argument
            and should return a boolean.

        :param message_store_factory: Allows you to specify a message store
            factory for all channel of this `case`.

        :return: one channel by condition parameter.
        """
        if names is None:
            names = [None] * len(conditions)

        c = Case(*conditions, names=names, parent_channel=self,
                 message_store_factory=message_store_factory, loop=self.loop)
        self._nodes.append(c)
        return [chan for cond, chan in c.cases]

    def handle_and_wait(self, msg):
        """ Handle a message synchronously. Mainly used for testing purpose.

        :param msg: Message to process
        :return: Processed message.
        """
        return self.loop.run_until_complete(self.handle(msg))

    async def handle(self, msg):
        """ Overload this method only if you know what you are doing but
        call it from child class to add behaviour.

        :param msg: To be processed msg.
        :return: Processed message
        """

        # Store message before any processing
        # TODO If store fails, do we stop processing ?
        # TODO Do we store message even if channel is stopped ?
        msg_store_id = await self.message_store.store(msg)

        if self.status in [BaseChannel.STOPPED, BaseChannel.STOPPING]:
            raise ChannelStopped("Channel is stopped so you can't send message.")

        self.logger.info("%s handle %s", self, msg)

        # Only one message processing at time
        with (await self.lock):
            self.status = BaseChannel.PROCESSING
            try:
                result = await self.subhandle(msg)
                await self.message_store.change_message_state(msg_store_id, message.Message.PROCESSED)
                return result
            except Dropped:
                await self.message_store.change_message_state(msg_store_id, message.Message.PROCESSED)
                raise
            except Rejected:
                await self.message_store.change_message_state(msg_store_id, message.Message.REJECTED)
                raise
            except Exception:
                self.logger.exception('Error while processing message %s', msg)
                await self.message_store.change_message_state(msg_store_id, message.Message.ERROR)
                raise
            finally:
                self.status = BaseChannel.WAITING
                self.processed_msgs += 1

    async def subhandle(self, msg):
        """ Overload this method only if you know what you are doing. Called by ``handle`` method.

        :param msg: To be processed msg.

        :return: Processed message
        """

        result = await self.process(msg)

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
                result = await self.next_node.handle(result)

        return result

    async def process(self, msg):
        """ Overload this method only if you know what you are doing. Called by ``subhandle`` method.

        :param msg: To be processed msg.

        :return: Processed message
        """

        if self._nodes:
            res = await self._nodes[0].handle(msg)
            return res
        else:
            return msg

    async def replay(self, msg_id):
        """
        This method allows you to replay a message from channel `message_store`.

        :param msg_id: Message id to replay.

        :return: The result of the processing.
        """
        msg_dict = await self.message_store.get(msg_id)
        new_message = msg_dict['message'].renew()
        result = await self.handle(new_message)
        return result

    def to_dict(self):
        return {
            'name': self.name,
            'status': BaseChannel.status_id_to_str(self.status),
            'has_message_store': not isinstance(self.message_store, msgstore.NullMessageStore),
            'processed': self.processed_msgs,
        }

    def subchannels(self):
        res = []

        for node in self._nodes:
            if isinstance(node, SubChannel) or isinstance(node, ConditionSubChannel):
                chan_dict = node.to_dict()
                chan_dict['subchannels'] = node.subchannels()
                res.append(chan_dict)
            elif isinstance(node, Case):
                for cond, channel in node.cases:
                    chan_dict = channel.to_dict()
                    chan_dict['subchannels'] = channel.subchannels()
                    res.append(chan_dict)
        return res

    def graph(self, prefix='', dot=False):
        """
        Generate a text graph for this channel.
        """
        for node in self._nodes:
            if isinstance(node, SubChannel):
                print(prefix + '|—\\ (%s)' % node.name)
                node.graph(prefix='|  ' + prefix)
            elif isinstance(node, ConditionSubChannel):
                print(prefix + '|?\\ (%s)' % node.name)
                node.graph(prefix='|  ' + prefix)
                print(prefix + '|  -> Out')
            elif isinstance(node, Case):
                for i, c in enumerate(node.cases):
                    print(prefix + '|c%s\\' % i)
                    c[1].graph(prefix='|  ' + prefix)
                    print(prefix + '|<--')
            else:
                print(prefix + '|-' + node.name)

    def graph_dot(self, end=''):
        """
        Generate a compatible dot graph for this channel.
        """
        after = []
        cases = None

        print('#---')

        previous = self.name

        if end == '':
            end = self.name

        for node in self._nodes:
            if isinstance(node, SubChannel):
                print('"%s"->"%s";' % (previous, node.name))
                after.append((None, node))

            elif isinstance(node, ConditionSubChannel):
                print('"%s"->"%s" [style=dotted];' % (previous, node.name))
                after.append((end, node))

            elif isinstance(node, Case):
                cases = [c[1] for c in node.cases]

            else:
                if cases:
                    for c in cases:
                        print('"%s"->"%s" [style=dotted];' % (previous, c.name))
                        after.append((node.name, c))
                    cases = None

                else:
                    print('"%s"->"%s";' % (previous, node.name))

                previous = node.name

        if end:
            print('"%s"->"%s";' % (previous, end))

        for end, sub in after:
            sub.graph_dot(end=end)

    def __str__(self):
        return "<chan: %s>" % self.name


class SubChannel(BaseChannel):
    """ Subchannel used for forking channel processing. """

    def callback(self, fut):
        try:
            result = fut.result()
            logger.debug("Subchannel %s end process message %s", self, result)
        except Exception:
            self.logger.exception("Error while processing msg in subchannel %s", self)

    async def process(self, msg):
        if self._nodes:
            fut = ensure_future(self._nodes[0].handle(msg.copy()), loop=self.loop)
            fut.add_done_callback(self.callback)

        return msg


class ConditionSubChannel(BaseChannel):
    """
    ConditionSubchannel used for make alternative path. This processing replace
    all further channel processing.
    """

    def __init__(self, condition=lambda x: True, **kwargs):
        super().__init__(**kwargs)
        self.condition = condition

    def test_condition(self, msg):
        if callable(self.condition):
            return self.condition(msg)
        else:
            return self.condition

    async def subhandle(self, msg):
        if self.test_condition(msg):
            result = await self.process(msg)
        else:
            if self.next_node:
                result = await self.next_node.handle(msg)
            else:
                result = msg

        return result


class Case():
    """ Case node internally used for `.case()` BaseChannel method. Don't use it.
    """
    def __init__(self, *args, names=None, parent_channel=None, message_store_factory=None, loop=None):
        self.next_node = None
        self.cases = []

        if names is None:
            names = []

        self.loop = loop or asyncio.get_event_loop()

        if message_store_factory is None:
            message_store_factory = msgstore.NullMessageStoreFactory()

        for cond, name in zip(args, names):
            b = BaseChannel(name=name, parent_channel=parent_channel,
                            message_store_factory=message_store_factory,
                            loop=self.loop)
            self.cases.append((cond, b))

    def _reset_test(self):
        for c in self.cases:
            c[1]._reset_test()

    def test_condition(self, condition, msg):
        if callable(condition):
            return condition(msg)
        else:
            return condition

    async def handle(self, msg):
        result = msg
        for cond, channel in self.cases:
            if self.test_condition(cond, msg):
                result = await channel.handle(msg)
                break

        if self.next_node:
            result = await self.next_node.handle(result)

        return result


class FileWatcherChannel(BaseChannel):
    """
    Watch for file change or creation. File content becomes message payload.
    ``filepath`` is in message meta.

    """

    NEW, UNCHANGED, MODIFIED, DELETED = range(4)

    def __init__(self, *args, basedir='', regex='.*', interval=1, binary_file=False, path='', **kwargs):
        super().__init__(*args, **kwargs)
        if path:
            self.basedir = path
            warnings.warn("path deprecated, use basedir instead", DeprecationWarning)
        if basedir:
            self.basedir = basedir
        self.regex = regex
        self.interval = interval
        self.dirflag = os.path.isdir(self.basedir)
        self.data = {}
        self.re = re.compile(self.regex)
        self.binary_file = binary_file

        # Set mtime for all existing matching files
        if os.path.exists(self.basedir):
            for filename in os.listdir(self.basedir):
                if self.re.match(filename):
                    filepath = os.path.join(self.basedir, filename)
                    mtime = os.stat(filepath).st_mtime
                    self.data[filename] = mtime
        else:
            self.logger.warning("Path doesn't exists: %r", self.basedir)

    async def start(self):
        await super().start()
        ensure_future(self.watch_for_file(), loop=self.loop)

    def file_status(self, filename):
        if filename in self.data:
            old_mtime = self.data[filename]
            filepath = os.path.join(self.basedir, filename)
            new_mtime = os.stat(filepath).st_mtime
            if new_mtime == old_mtime:
                return FileWatcherChannel.UNCHANGED
            elif new_mtime > old_mtime:
                return FileWatcherChannel.MODIFIED
        else:
            return FileWatcherChannel.NEW

    def _handle_callback(self, future):
        try:
            future.result()
        except Dropped:
            pass

    async def watch_for_file(self):
        # TODO cancel sleep on channel stopping
        await asyncio.sleep(self.interval, loop=self.loop)
        try:
            if os.path.exists(self.basedir):
                listfile = os.listdir(self.basedir)
                listfile.sort()

                for filename in listfile:
                    if self.re.match(filename):
                        status = self.file_status(filename)

                        if status in [FileWatcherChannel.MODIFIED, FileWatcherChannel.NEW]:
                            filepath = os.path.join(self.basedir, filename)
                            self.data[filename] = os.stat(filepath).st_mtime

                            # Read file and make message
                            if self.binary_file:
                                mode = "rb"
                            else:
                                mode = "r"

                            with open(filepath, mode) as f:
                                msg = message.Message()
                                msg.payload = f.read()
                                msg.meta['filename'] = filename
                                msg.meta['filepath'] = filepath
                                fut = ensure_future(self.handle(msg), loop=self.loop)
                                fut.add_done_callback(self._handle_callback)

        except Exception:  # TODO: might explicitely silence some special cases.
            self.logger.exception("filewatcher problem")
        finally:
            if self.status not in (BaseChannel.STOPPING, BaseChannel.STOPPED,):
                ensure_future(self.watch_for_file(), loop=self.loop)


from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.hl7', 'MLLPChannel', ['hl7'])
wrap.add_lazy('pypeman.contrib.http', 'HttpChannel', ['aiohttp'])
wrap.add_lazy('pypeman.contrib.time', 'CronChannel', ['aiocron'])
wrap.add_lazy('pypeman.contrib.ftp', 'FTPWatcherChannel', [])
