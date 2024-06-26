import asyncio
import contextvars
import logging
import re
import traceback
import types
import uuid
import warnings

from pathlib import Path

from pypeman import message, msgstore, events
from pypeman.errors import PypemanConfigError
from pypeman.helpers.itertools import flatten
from pypeman.helpers.sleeper import Sleeper


logger = logging.getLogger(__name__)

# List all channel registered
all_channels = []

_channel_names = set()

MSG_CTXVAR = contextvars.ContextVar("msg")


class Dropped(Exception):
    """ Used to stop process as message is processed. Default success should be returned.
    """


class Rejected(Exception):
    """ Used to tell caller the message is invalid with a error return.
    """


class ChannelStopped(Exception):
    """ The channel is stopped and can't process message.
    """


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

    :param wait_subchans: Boolean, if set to True, channels will wait for suchannel ends for sending,
    speed response and process a new message
    """
    STARTING, WAITING, PROCESSING, STOPPING, STOPPED = range(5)
    STATE_NAMES = ['STARTING', 'WAITING', 'PROCESSING', 'STOPPING', 'STOPPED']

    def __init__(self, name=None, parent_channel=None, loop=None, message_store_factory=None,
                 wait_subchans=False, verbose_name=None):

        self.uuid = uuid.uuid4()

        all_channels.append(self)
        self._nodes = []
        self._node_map = {}
        self._status = BaseChannel.STOPPED
        self.processed_msgs = 0
        self.interruptable_sleeper = Sleeper(loop)  # for interruptable sleeps
        self.join_nodes = None
        self.fail_nodes = None
        self.drop_nodes = None
        self.reject_nodes = None
        self.final_nodes = None
        self.wait_subchans = wait_subchans
        self.raise_dropped = False

        if name:
            self.name = name
        else:
            warnings.warn(
                "Channels without names are deprecated and will be removed in version 0.5.2",
                DeprecationWarning)
            self.name = self.__class__.__name__ + "_" + str(len(all_channels))

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

        if self.name in _channel_names:
            raise NameError(
                "Duplicate channel name %r . "
                "Channel names must be unique !" % self.name
            )
        if verbose_name:
            self.verbose_name = verbose_name
        else:
            self.verbose_name = self.name

        _channel_names.add(self.name)

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
        # Lock use `asyncio.get_running_loop()` that only be called from coroutines or callbacks
        # So now, lock is instanciated at start
        self.lock = None

        self.sub_chan_tasks = []
        self.sub_chan_endnodes = []

    def _reset_sub_chan_endnodes(self, fut):
        """
        Remove all subchan callbacks from the list
        called by the done_callback of subchan future callbacks
        """
        self.sub_chan_endnodes = []
        fut.result()

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
        asyncio.create_task(events.channel_change_state.fire(
            channel=self, old_state=old_state, new_state=value))

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
        self.logger.debug("Channel %s starting ...", str(self))
        self.lock = asyncio.Lock()
        self.status = BaseChannel.STARTING
        if self._first_start:
            self.init_node_graph()
            self._first_start = False
        await self.message_store.start()
        self.status = BaseChannel.WAITING
        self.logger.info("Channel %s started", str(self))

    def init_node_graph(self):
        if self._nodes:
            previous_node = self._nodes[0]

            for node in self._nodes[1:]:
                previous_node.next_node = node
                previous_node = node

    async def stop(self):
        """
        Stop the channel.
        Called when
        - pypeman shuts down.
        - a channel is stopped (e.g. via the admin interface)
        """
        self.logger.debug("Channel %s stopping ...", str(self))
        self.status = BaseChannel.STOPPING
        # Verify that all messages are processed
        async with self.lock:
            self.status = BaseChannel.STOPPED
        # stop all pending sleeps
        await self.interruptable_sleeper.cancel_all()
        self.logger.info("Channel %s stopped", str(self))

    def _reset_test(self):
        """ Enable test mode and reset node data.
        """
        self.raise_dropped = True
        for node in self._nodes:
            node._reset_test()
        if self.join_nodes:
            for node in self.join_nodes:
                node._reset_test()
        if self.drop_nodes:
            for node in self.drop_nodes:
                node._reset_test()
        if self.reject_nodes:
            for node in self.reject_nodes:
                node._reset_test()
        if self.final_nodes:
            for node in self.final_nodes:
                node._reset_test()
        if self.fail_nodes:
            for node in self.fail_nodes:
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

        :param args: Nodes to add. can be a list, a tupe or a nested
                list of tuples or lists if nodes.
                nodes with value None are ignored
        """
        args = [node for node in flatten(args) if node]
        for node in args:
            node.channel = self
            self._nodes.append(node)
            self._register_node(node)

        return self

    def fork(self, name=None, message_store_factory=None):
        """
        Create a new channel that processes a copy of the message
        at this point.
        Subchannels are executed in parallel of main process.

        :return: The forked channel
        """

        s = SubChannel(
            name=name, parent_channel=self,
            message_store_factory=message_store_factory, loop=self.loop,
            wait_subchans=self.wait_subchans)
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
            message_store_factory=message_store_factory, loop=self.loop,
            wait_subchans=self.wait_subchans)
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
                 message_store_factory=message_store_factory, loop=self.loop,
                 wait_subchans=self.wait_subchans)
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
        self.logger.info("chan %s handle %s", str(self), str(msg))
        has_callback = hasattr(self, "_callback")
        setattr(msg, "chan_rslt", None)
        setattr(msg, "chan_exc", None)
        setattr(msg, "chan_exc_traceback", None)
        # Only one message processing at time
        async with self.lock:
            self.status = BaseChannel.PROCESSING
            try:
                result = await self.subhandle(msg.copy())
                await self.message_store.change_message_state(msg_store_id, message.Message.PROCESSED)
                msg.chan_rslt = result
                if self.join_nodes and not has_callback:
                    await self.join_nodes[0].handle(result.copy())
                return result
            except Dropped as exc:
                self.logger.info("%s DROP msg %s", str(self), str(msg))
                msg.chan_exc = exc
                msg.chan_exc_traceback = traceback.format_exc()
                await self.message_store.change_message_state(msg_store_id, message.Message.PROCESSED)
                if self.drop_nodes and not has_callback:
                    await self.drop_nodes[0].handle(msg.copy())
                if self.raise_dropped:
                    raise
                return msg
            except Rejected as exc:
                self.logger.info("%s REJECT msg %s", str(self), str(msg))
                msg.chan_exc = exc
                msg.chan_exc_traceback = traceback.format_exc()
                await self.message_store.change_message_state(msg_store_id, message.Message.REJECTED)
                await self.message_store.add_message_meta_infos(msg_store_id, "err_msg", str(exc))
                if self.reject_nodes and not has_callback:
                    await self.reject_nodes[0].handle(msg.copy())
                raise
            except Exception as exc:
                msg.chan_exc = exc
                msg.chan_exc_traceback = traceback.format_exc()
                self.logger.error('Error while processing message %s (chan %s)', str(msg), str(self))
                await self.message_store.change_message_state(msg_store_id, message.Message.ERROR)
                await self.message_store.add_message_meta_infos(msg_store_id, "err_msg", str(exc))
                if self.fail_nodes and not has_callback:
                    await self.fail_nodes[0].handle(msg.copy())
                raise
            finally:
                self.status = BaseChannel.WAITING
                self.processed_msgs += 1
                if self.final_nodes and not has_callback:
                    await self.final_nodes[0].handle(msg.copy())
                try:
                    if self.sub_chan_tasks:
                        # Launch sub chans handle()
                        subchantasks = asyncio.gather(*self.sub_chan_tasks)
                        if self.wait_subchans:
                            await subchantasks
                finally:
                    if self.sub_chan_endnodes:
                        # Launch and wait for sub chans callbacks
                        subchan_endnodes_fut = asyncio.gather(*self.sub_chan_endnodes)
                        subchan_endnodes_fut.add_done_callback(self._reset_sub_chan_endnodes)
                        if self.wait_subchans:
                            await subchan_endnodes_fut
                    self.logger.info("%s end handle %s", str(self), str(msg))

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
        self.logger.info("try to replay %s", str(msg_id))
        msg_dict = await self.message_store.get(msg_id)
        new_message = msg_dict['message'].renew()
        result = await self.handle(new_message)
        return result

    def to_dict(self):
        return {
            'name': self.name,
            'verbose_name': self.verbose_name,
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
        # TODO: ask to klaus how to implement and show endnodes in the graph
        for node in self._nodes:
            if isinstance(node, SubChannel):
                yield prefix + '|—\\ (%s)' % node.name
                for entry in node.graph(prefix='|  ' + prefix):
                    yield entry
            elif isinstance(node, ConditionSubChannel):
                yield prefix + '|?\\ (%s)' % node.name
                for entry in node.graph(prefix='|  ' + prefix):
                    yield entry
                yield prefix + '|  -> Out'
            elif isinstance(node, Case):
                for i, c in enumerate(node.cases):
                    yield prefix + '|c%s\\' % i
                    for entry in c[1].graph(prefix='|  ' + prefix):
                        yield entry
                    yield prefix + '|<--'
            else:
                yield prefix + '|-' + node.name

    def graph_dot(self, end=''):
        """
        Generate a compatible dot graph for this channel.
        """
        after = []
        cases = None

        yield '#---'

        previous = self.name

        if end == '':
            end = self.name

        for node in self._nodes:
            if isinstance(node, SubChannel):
                yield '"%s"->"%s";' % (previous, node.name)
                after.append((None, node))

            elif isinstance(node, ConditionSubChannel):
                yield '"%s"->"%s" [style=dotted];' % (previous, node.name)
                after.append((end, node))

            elif isinstance(node, Case):
                cases = [c[1] for c in node.cases]

            else:
                if cases:
                    for c in cases:
                        yield '"%s"->"%s" [style=dotted];' % (previous, c.name)
                        after.append((node.name, c))
                    cases = None

                else:
                    yield '"%s"->"%s";' % (previous, node.name)

                previous = node.name

        if end:
            yield '"%s"->"%s";' % (previous, end)

        for end, sub in after:
            for entry in sub.graph_dot(end=end):
                yield entry

    def _init_end_nodes(self, *end_nodes):
        """
        join/fail/drop/reject/final nodes are nodes that are launched
        after channel subhandle
        This func permits to chain them without adding them at the end of self._nodes
        """
        end_nodes = [node for node in flatten(end_nodes) if node]
        for node in end_nodes:
            node.channel = self

        if len(end_nodes) > 1:
            previous_node = end_nodes[0]

            for node in end_nodes[1:]:
                previous_node.next_node = node
                previous_node = node
        return end_nodes

    def add_join_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a successful channel process
        The first node take the result message of the channel as input

        CAUTION: TODO: BUG if an exception occurs during processing of join_nodes there's
        a different comportment if it's a BaseChannel or a SubChannel (due to implementation)
            - BaseChannel: if there's fail/drop/reject_nodes, they will be launched
            - SubChannel: no other endnodes will be launched (except final_nodes)
        """
        if self.join_nodes:
            raise PypemanConfigError(f"join_nodes already existing for channel {self.name}")
        self.join_nodes = self._init_end_nodes(*end_nodes)

    def add_fail_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises an Exception
        (except Dropped and Rejected)
        The first node take the entry message of the channel as input
        """
        if self.fail_nodes:
            raise PypemanConfigError(f"fail_nodes already existing for channel {self.name}")
        self.fail_nodes = self._init_end_nodes(*end_nodes)

    def add_drop_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises a Dropped
        The first node take the entry message of the channel as input
        """
        if self.drop_nodes:
            raise PypemanConfigError(f"drop_nodes already existing for channel {self.name}")
        self.drop_nodes = self._init_end_nodes(*end_nodes)

    def add_reject_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises a Rejected
        The first node take the entry message of the channel as input
        """
        if self.reject_nodes:
            raise PypemanConfigError(f"reject_nodes already existing for channel {self.name}")
        self.reject_nodes = self._init_end_nodes(*end_nodes)

    def add_final_nodes(self, *end_nodes):
        """
        Add nodes that will be launched all the time after a channel process
        The first node take the entry message of the channel as input (TODO: ask if it's ok)
        """
        if self.final_nodes:
            raise PypemanConfigError(f"final_nodes already existing for channel {self.name}")
        self.final_nodes = self._init_end_nodes(*end_nodes)

    # def _callback(self, future):
    #     """
    #     Function called by subchannel future done_callback
    #     If this function is implemented join/fail/reject/drop/final endnodes are not instanciated
    #     in handle but in this func so take care to not implement it if you are not sure what
    #     you're doing
    #     TODO: Maybe add a warning if _callback and not isinstance(SubChannel)  ??
    #     """
    #     pass

    def __str__(self):
        return "<chan: %s>" % self.name

    def __repr__(self):
        typ = type(self)
        return "<%s.%s:(\"%s\")>" % (typ.__module__, typ.__name__, self.name)


def reset_pypeman_channels():
    """
    clears book keeping of all channels

    Can be useful for unit testing.
    """
    logger.info("clearing all_channels and _channel-names.")
    all_channels.clear()
    _channel_names.clear()


class SubChannel(BaseChannel):
    """ Subchannel used for forking channel processing. """

    def _callback(self, fut):
        """
        """
        ctx = contextvars.copy_context()
        entrymsg = ctx.get(MSG_CTXVAR)
        setattr(entrymsg, "chan_rslt", None)
        setattr(entrymsg, "chan_exc", None)
        setattr(entrymsg, "chan_exc_traceback", None)
        endnodes_tasks = []
        try:
            result = fut.result()
            entrymsg.chan_rslt = result
            if self.join_nodes:
                endnode_task = asyncio.create_task(self.join_nodes[0].handle(result.copy()))
                endnodes_tasks.append(endnode_task)
            logger.info(
                "Subchannel %s end process message %s, rslt is msg %s",
                str(self), str(entrymsg), str(result))
        except Dropped as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.drop_nodes:
                endnode_task = asyncio.create_task(self.drop_nodes[0].handle(entrymsg.copy()))
                endnodes_tasks.append(endnode_task)
            self.logger.info("Subchannel %s. Msg %s was dropped", str(self), str(entrymsg))
        except Rejected as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.reject_nodes:
                endnode_task = asyncio.create_task(self.reject_nodes[0].handle(entrymsg.copy()))
                endnodes_tasks.append(endnode_task)
            self.logger.info("Subchannel %s. Msg %s was Rejected", str(self), str(entrymsg))
            raise
        except Exception as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.fail_nodes:
                endnode_task = asyncio.create_task(self.fail_nodes[0].handle(entrymsg.copy()))
                endnodes_tasks.append(endnode_task)
            self.logger.exception(
                "Error while processing msg %s in subchannel %s", str(entrymsg), str(self))
            raise
        finally:
            if self.final_nodes:
                endnode_task = asyncio.create_task(self.final_nodes[0].handle(entrymsg.copy()))
                endnodes_tasks.append(endnode_task)
            self.parent.sub_chan_endnodes.extend(endnodes_tasks)
            self.parent.sub_chan_tasks.remove(fut)
            self.logger.info(
                "subchan %s end process msg %s", str(self), str(entrymsg))

    async def process(self, msg):
        if self._nodes:
            msgctxvartoken = MSG_CTXVAR.set(msg.copy())
            ctx = contextvars.copy_context()
            fut = asyncio.create_task(self._nodes[0].handle(msg.copy()))
            fut.add_done_callback(self._callback, context=ctx)
            self.parent.sub_chan_tasks.append(fut)
            MSG_CTXVAR.reset(msgctxvartoken)
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
        result = await self.process(msg)
        return result

    async def handle(self, msg):
        if self.test_condition(msg):
            result = await super().handle(msg)
        else:
            if self.next_node:
                result = await self.next_node.handle(msg)
            else:
                result = msg

        return result


class Case():
    """ Case node internally used for `.case()` BaseChannel method. Don't use it.
    """
    def __init__(self, *args, names=None, parent_channel=None, message_store_factory=None, loop=None,
                 wait_subchans=False):
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
                            loop=self.loop, wait_subchans=wait_subchans)
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


class MergeChannel(BaseChannel):
    """
    This class permits to have multiple channel classes (watchers/listeners/..)
    as inputs to a single channel
    TODO: CAUTION: Not sure that input channels works with end nodes + they don't
        appears in graph

    TODO: check why we need
        if channel.loop != self.loop:
            channel.loop = self.loop
    There's a bug with event loop (maybe only on tests) but weird
    (traceback: RuntimeError: Task <Task pending name='Task-126' coro=<MergeChannel.start()
    running at
    /home/quentin/projects/interop2/app/vendor/common/custom_channels.py:51>
    cb=[_run_until_complete_cb()
    at /home/quentin/.pyenv/versions/3.10.10/lib/python3.10/asyncio/base_events.py:184]>
    got Future
    <Future pending cb=[_chain_future.<locals>._call_check_cancel() at
    /home/quentin/.pyenv/versions/3.10.10/lib/python3.10/asyncio/futures.py:385]>
    attached to a different loop
    )

    Args:
        channels (list of channels): List of channels to use as inputs
    """

    def __init__(self, *args, chans, **kwargs):
        super().__init__(*args, **kwargs)
        self.channels = chans
        for channel in self.channels:
            all_channels.remove(channel)
            channel.add = None
            channel.handle = self.handle
            channel.handle_and_wait = self.handle_and_wait
            if channel.loop != self.loop:
                channel.loop = self.loop

    async def start(self):
        for channel in self.channels:
            if channel._nodes:
                raise AttributeError(f"A merged channel cannot have nodes (chan {channel.name})")
            if channel.loop != self.loop:
                channel.loop = self.loop
            await channel.start()
        await super().start()

    async def stop(self):
        for channel in self.channels:
            if channel.loop != self.loop:
                channel.loop = self.loop
            await channel.stop()
        await super().stop()


class FileWatcherChannel(BaseChannel):
    """
    Watch for file change or creation. File content becomes message payload.
    ``filepath`` is in message meta.

    If the regex is for an acknowledgement file (.ok for example) you can convert it to the
    real filepath via the real_extensions init arg. The returned msg payload will be the
    content of the real file and not the acknowledgement file. Idem for meta
    """

    NEW, UNCHANGED, MODIFIED, DELETED = range(4)

    def __init__(self, *args, basedir='', regex='.*', interval=1, binary_file=False, path='',
                 real_extensions=None, **kwargs):
        super().__init__(*args, **kwargs)
        if path:
            self.basedir = path
            warnings.warn("path deprecated, use basedir instead", DeprecationWarning)
        if basedir:
            self.basedir = basedir
        self.basedir = Path(self.basedir)
        self.regex = regex
        self.interval = interval
        self.dirflag = self.basedir.is_dir()
        self.data = {}
        self.re = re.compile(self.regex)
        self.binary_file = binary_file
        self.real_extensions = real_extensions  # list of extensions for exemple: [".csv", ".CSV"]
        # Set mtime for all existing matching files
        if self.basedir.exists():
            for filepath in self.basedir.iterdir():
                if self.re.match(filepath.name):
                    mtime = filepath.stat().st_mtime
                    self.data[filepath.name] = mtime
        else:
            self.logger.warning("Path doesn't exists: %r", self.basedir)

    async def start(self):
        await super().start()
        asyncio.create_task(self.watch_for_file())

    def file_status(self, filename):
        if filename in self.data:
            old_mtime = self.data[filename]
            filepath = self.basedir / filename
            new_mtime = filepath.stat().st_mtime
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
        # self.logger.debug("Will sleep")
        await self.interruptable_sleeper.sleep(self.interval)
        # await asyncio.sleep(self.interval, loop=self.loop)
        # self.logger.debug("sleep done")
        try:
            if self.basedir.exists():
                listfile = self.basedir.iterdir()

                for filepath in sorted(listfile):
                    filename = filepath.name
                    if self.re.match(filename):
                        status = self.file_status(filename)

                        if status in [FileWatcherChannel.MODIFIED, FileWatcherChannel.NEW]:
                            self.data[filename] = filepath.stat().st_mtime
                            if self.real_extensions:
                                for extension in self.real_extensions:
                                    real_fpath = filepath.with_suffix(extension)
                                    if real_fpath.exists():
                                        filepath = real_fpath
                                        filename = real_fpath.name
                                        break
                                else:
                                    # If no related files
                                    # TODO : ask if raise exc or not
                                    logger.error(
                                        "No %r related file to %s",
                                        self.real_extensions, str(filepath))
                                    continue

                            # Read file and make message
                            if self.binary_file:
                                mode = "rb"
                            else:
                                mode = "r"

                            msg = message.Message()

                            with filepath.open(mode) as f:
                                msg.payload = f.read()
                            msg.meta['filename'] = filename
                            msg.meta['filepath'] = str(filepath)
                            fut = asyncio.create_task(self.handle(msg))
                            fut.add_done_callback(self._handle_callback)

        except Exception:  # TODO: might explicitely silence some special cases.
            self.logger.exception("filewatcher problem")
        finally:
            if self.status not in (BaseChannel.STOPPING, BaseChannel.STOPPED,):
                asyncio.create_task(self.watch_for_file())
            else:
                logger.warning("Won't watch anymore")


from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.hl7', 'MLLPChannel', ['hl7'])
wrap.add_lazy('pypeman.contrib.http', 'HttpChannel', ['aiohttp'])
wrap.add_lazy('pypeman.contrib.time', 'CronChannel', ['aiocron'])
wrap.add_lazy('pypeman.contrib.ftp', 'FTPWatcherChannel', [])
