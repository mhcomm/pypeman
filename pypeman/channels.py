import asyncio
import contextvars
import logging
import re
import traceback
import types
import uuid
import warnings

from pathlib import Path

from pypeman import exceptions
from pypeman import conf
from pypeman import message, msgstore, events
from pypeman.exceptions import EndChanProcess
from pypeman.exceptions import Dropped
from pypeman.exceptions import Rejected
from pypeman.exceptions import ChannelStopped
from pypeman.helpers.itertools import flatten
from pypeman.helpers.sleeper import Sleeper
from pypeman.retry import RetryFileMsgStore


logger = logging.getLogger(__name__)

# List all channel registered
all_channels = []

_channel_names = set()

MSG_CTXVAR = contextvars.ContextVar("msg")


def get_channel(name):
    """Permits retrieving a channel by its name or short_name

    Args:
        name (str): The channel's name
    """
    for chan in all_channels:
        if chan.name == name or chan.short_name == name:
            return chan


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
    STARTING, WAITING, PROCESSING, STOPPING, STOPPED, PAUSED = range(6)
    STATE_NAMES = ['STARTING', 'WAITING', 'PROCESSING', 'STOPPING', 'STOPPED', 'PAUSED']

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
        self.init_nodes = None
        self.wait_subchans = wait_subchans
        self.raise_dropped = False

        if name:
            self.short_name = self.name = name
        else:
            warnings.warn(
                "Channels without names are deprecated and will be removed in version 0.5.2",
                DeprecationWarning)
            self.short_name = self.name = self.__class__.__name__ + "_" + str(len(all_channels))

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

        _channel_names.add(self.short_name)

        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

        self.logger = logging.getLogger('pypeman.channels.%s' % self.name)

        self.next_node = None

        self.message_store_factory = message_store_factory or msgstore.NullMessageStoreFactory()
        self.has_message_store = not isinstance(
            self.message_store_factory,
            msgstore.NullMessageStoreFactory,
        )
        self.message_store = self.message_store_factory.get_store(self.name)
        if conf.SETTINGS_IMPORTED:
            retry_store_path = conf.settings.RETRY_STORE_PATH
        else:
            logger.warning(
                "Caution, settings not imported before chan init"
            )
            retry_store_path = None
        if retry_store_path is not None:
            self.retry_store = RetryFileMsgStore(
                path=retry_store_path,
                store_id=self.name,
                channel=self,
            )
        else:
            self.retry_store = None

        self._first_start = True

        # Used to avoid multiple messages processing at same time
        # Lock use `asyncio.get_running_loop()` that only be called from coroutines or callbacks
        # So now, lock is instanciated at start
        self.lock = None

        self.sub_chan_tasks = []
        self.sub_chan_endnodes = []

        # this is a dictionary that can be set/extended outside the class
        # to influence the `to_dict` method; it is mainly relevant in extending
        # the serialization of custom channels which are not necessarily child
        # classes, for example with remoteadmin's `list_channels`
        self.extra_dict = {}

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
        if self.retry_store:
            await self.retry_store.start()

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
        if self.retry_store:
            await self.retry_store.stop()

    def _reset_test(self):
        """ Enable test mode and reset node data.
        """
        self.raise_dropped = True
        for node in self._nodes:
            node._reset_test()
        if self.init_nodes:
            for node in self.init_nodes:
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
        if self.retry_store:
            self.retry_store._reset_test()

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
        node_map = self._node_map
        if self.init_nodes:
            for node in self.init_nodes:
                node_map[node.name] = node
        if self.join_nodes:
            for node in self.join_nodes:
                node_map[node.name] = node
        if self.drop_nodes:
            for node in self.drop_nodes:
                node_map[node.name] = node
        if self.reject_nodes:
            for node in self.reject_nodes:
                node_map[node.name] = node
        if self.fail_nodes:
            for node in self.fail_nodes:
                node_map[node.name] = node
        if self.final_nodes:
            for node in self.final_nodes:
                node_map[node.name] = node
        return node_map.get(name)

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

    def _has_callback(self):
        return hasattr(self, "_callback")

    async def _call_special_nodes(self, msg, node_type, start_nodename=None):
        """
            Permits calling init/join/drop/reject/fail/final nodes

            Args:
                msg (message.Message): The message to pass to nodes
                node_type (str): The special node type (init|join|drop|reject|fail|final)
                start_nodename (str, optional): The node name where to inject. Defaults to None.
        """
        node_type_to_nodelist = {
            "init": self.init_nodes,
            "join": self.join_nodes,
            "drop": self.drop_nodes,
            "reject": self.reject_nodes,
            "fail": self.fail_nodes,
            "final": self.final_nodes,
        }
        nodelist = node_type_to_nodelist[node_type]
        if nodelist:
            if start_nodename:
                start_node = self.get_node(start_nodename)
                if start_node not in nodelist:
                    raise ValueError(f"Node {start_nodename} not in {node_type} nodes")
            else:
                start_node = nodelist[0]
            idx_start_node = nodelist.index(start_node)
            nodes = nodelist[idx_start_node:]
            msg = await self._process_nodes(nodes=nodes, msg=msg.copy(), add_sub_state=True)
        return msg

    async def _call_base_handling(self, msg, start_nodename=None, call_endnodes=True, set_state=True):
        """
        Process message from the given node
            start_nodename must refer to a "classic" node, not init or end node

        Args:
            msg (message.Message): To be processed msg.
            start_nodename (str, optional): the nodename where you want to
                inject the message, if no nodename is passed, process it from
                the channel's start. You could set it to "_initial", the message will be processed
                from the start but will bypass init_nodes
                Defaults to None.
            call_endnodes (bool, default=True): Flag to indicate if endnodes have to be called
                or not
            set_state (bool, default=True): Flag to indicate if the final message state have to be set or not
        """
        retry_exc_catched = None
        try:
            if not start_nodename:
                msg = await self._call_special_nodes(msg=msg, node_type="init")
            result = await self.subhandle(msg.copy(), start_nodename=start_nodename)
            msg.chan_rslt = result
            if not self._has_callback() and call_endnodes:
                await self._call_special_nodes(msg=result, node_type="join")
            return result
        except Dropped as exc:
            self.logger.info("%s DROP msg %s", str(self), str(msg))
            msg.chan_exc = exc
            msg.chan_exc_traceback = traceback.format_exc()
            if not self._has_callback() and call_endnodes:
                try:
                    await self._call_special_nodes(msg=msg, node_type="drop")
                except exceptions.RetryException as retry_exc:
                    self.logger.info("%s CATCH RETRY in drop nodes for msg %s", str(self), str(msg))
                    retry_exc_catched = retry_exc
            if self.raise_dropped:
                raise
            return msg
        except Rejected as exc:
            self.logger.info("%s REJECT msg %s", str(self), str(msg))
            msg.chan_exc = exc
            msg.chan_exc_traceback = traceback.format_exc()
            await self.message_store.add_message_meta_infos(msg.store_id, "err_msg", str(exc))
            if not self._has_callback() and call_endnodes:
                try:
                    await self._call_special_nodes(msg=msg, node_type="reject")
                except exceptions.RetryException as retry_exc:
                    self.logger.info("%s CATCH RETRY in reject nodes for msg %s", str(self), str(msg))
                    retry_exc_catched = retry_exc
            raise
        except (exceptions.RetryException, exceptions.PausedChanException) as exc:
            self.logger.info("%s CATCH RETRY for msg %s", str(self), str(msg))
            retry_exc_catched = exc
            await self.message_store.change_message_state(msg.store_id, message.Message.WAIT_RETRY)
            raise exc
        except Exception as exc:
            msg.chan_exc = exc
            msg.chan_exc_traceback = traceback.format_exc()
            self.logger.error('Error while processing message %s (chan %s)', str(msg), str(self))
            await self.message_store.add_message_meta_infos(msg.store_id, "err_msg", str(exc))
            if not self._has_callback() and call_endnodes:
                try:
                    await self._call_special_nodes(msg=msg, node_type="fail")
                except exceptions.RetryException as retry_exc:
                    self.logger.info("%s CATCH RETRY in fail nodes for msg %s", str(self), str(msg))
                    retry_exc_catched = retry_exc
            raise
        finally:
            if set_state and not retry_exc_catched and self.has_message_store:
                await self.message_store.set_state_to_worst_sub_state(msg.store_id)
            if not retry_exc_catched:
                if not self._has_callback() and call_endnodes:
                    await self._call_special_nodes(msg=msg, node_type="final")
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
            if retry_exc_catched:
                raise retry_exc_catched

    async def _get_base_msg_from_child(self, msg):
        """
        Get a message in the message store from one of its child

        Args:
            msg (message.Message): The child message

        Returns:
            message.Message: The base/parent message
        """
        base_msg_data = await self.message_store.get(msg.store_id)
        base_msg = base_msg_data["message"]
        base_msg.store_id = msg.store_id
        base_msg.store_chan_name = self.short_name
        return base_msg

    async def inject(self, msg, start_nodename, call_endnodes=True, set_state=True):
        """
        Inject a message at a given node name

        Args:
            msg (message.Message): Message to inject
            start_nodename (str): Node name where inject the message
            call_endnodes (bool, default=True): Flag to indicate if endnodes have to be called
                or not
            set_state (bool, default=True): Flag to indicate if the final message state have to be set or not
        """
        logger.debug(f"{self.short_name} Inject {msg} in {start_nodename}")

        async with self.lock:
            if not start_nodename or start_nodename == "_initial":
                # If start nodename is None inject at the channel's startpoint, if it's
                # "_initial", inject at startpoint too but bypass init_nodes
                # Return the resulted message
                result = await self._call_base_handling(
                    msg=msg, start_nodename=start_nodename, call_endnodes=call_endnodes,
                    set_state=set_state)
                return result

            start_node = self.get_node(name=start_nodename)
            if not start_node:
                raise ValueError("Node %s not found", start_nodename)
            logger.debug("Will inject msg %r in node %r", msg, start_node)
            if self.init_nodes and start_node in self.init_nodes:
                # Inject in specific init_node, then run the process and returns resulting message
                msg = await self._call_special_nodes(
                    msg=msg, node_type="init", start_nodename=start_nodename)
                result = await self._call_base_handling(
                    msg=msg, start_nodename="_initial",
                    call_endnodes=call_endnodes, set_state=set_state)
                return result
            elif start_node in self._nodes:
                # Inject in specific "processing" node and returns the resulting message
                result = await self._call_base_handling(
                    msg=msg, start_nodename=start_nodename,
                    call_endnodes=call_endnodes, set_state=set_state
                )
                return result

            if set_state and self.has_message_store:
                # You enter in this condition when start_nodename is not in init_nodes or "classic"
                # nodes (so it is in end nodes)
                # TODO: Must set the final state after end nodes
                await self.message_store.set_state_to_worst_sub_state(msg.store_id)

            # TODO: there's a difference between the inject and the classic handle in
            # how endnodes are processed:
            # In handle, if an error occur during join nodes call, it'll call fail/drop/reject
            # nodes, here it's not the case
            # I don't actually know if I have to change the handle or the inject
            if self.join_nodes and start_node in self.join_nodes:
                # Inject in specific join node (no return) (+ call of final nodes)
                exc_to_raise = None
                try:
                    await self._call_special_nodes(msg=msg, node_type="join", start_nodename=start_nodename)
                except exceptions.RetryException as exc:
                    raise exc
                except Exception as exc:
                    exc_to_raise = exc
                base_msg = await self._get_base_msg_from_child(msg)
                await self._call_special_nodes(msg=base_msg, node_type="final")
                if exc_to_raise is not None:
                    raise exc_to_raise
            elif self.drop_nodes and start_node in self.drop_nodes:
                # Inject in specific drop node (no return) (+ call of final nodes)
                exc_to_raise = None
                try:
                    await self._call_special_nodes(msg=msg, node_type="drop", start_nodename=start_nodename)
                except exceptions.RetryException as exc:
                    raise exc
                except Exception as exc:
                    exc_to_raise = exc
                base_msg = await self._get_base_msg_from_child(msg)
                await self._call_special_nodes(msg=base_msg, node_type="final")
                if exc_to_raise is not None:
                    raise exc_to_raise
            elif self.reject_nodes and start_node in self.reject_nodes:
                # Inject in specific reject node (no return) (+ call of final nodes)
                exc_to_raise = None
                try:
                    await self._call_special_nodes(
                        msg=msg, node_type="reject", start_nodename=start_nodename)
                except exceptions.RetryException as exc:
                    raise exc
                except Exception as exc:
                    exc_to_raise = exc
                base_msg = await self._get_base_msg_from_child(msg)
                await self._call_special_nodes(msg=base_msg, node_type="final")
                if exc_to_raise is not None:
                    raise exc_to_raise
            elif self.fail_nodes and start_node in self.fail_nodes:
                # Inject in specific fail node (no return) (+ call of final nodes)
                exc_to_raise = None
                try:
                    await self._call_special_nodes(msg=msg, node_type="fail", start_nodename=start_nodename)
                except exceptions.RetryException as exc:
                    raise exc
                except Exception as exc:
                    exc_to_raise = exc
                base_msg = await self._get_base_msg_from_child(msg)
                await self._call_special_nodes(msg=base_msg, node_type="final")
                if exc_to_raise is not None:
                    raise exc_to_raise
            elif self.final_nodes and start_node in self.final_nodes:
                # Inject in specific final node (no return)
                await self._call_special_nodes(msg=msg, node_type="final", start_nodename=start_nodename)
            else:
                raise Exception("Node %r found but not in init/handle/end nodes, weird..", start_node)

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
        if msg_store_id is not None:
            msg.store_id = msg_store_id
            msg.store_chan_name = self.short_name
        # TODO: Maybe think to add PENDING status to incoming message, this status is never set and
        # is currently unuseful. Uncomment next line if it's a good idea
        # await self.message_store.change_message_state(msg.store_id, message.Message.PENDING)

        if self.status in [BaseChannel.STOPPED, BaseChannel.STOPPING]:
            raise ChannelStopped("Channel is stopped so you can't send message.")
        self.logger.info("chan %s handle %s", str(self), str(msg))
        setattr(msg, "chan_rslt", None)
        setattr(msg, "chan_exc", None)
        setattr(msg, "chan_exc_traceback", None)
        # Only one message processing at time
        async with self.lock:
            if self.status == BaseChannel.PAUSED:
                await self.message_store.change_message_state(msg.store_id, message.Message.WAIT_RETRY)
                if self.retry_store:
                    await self.retry_store.store_until_retry(msg=msg, nodename=None)
                raise exceptions.PausedChanException(
                    "Channel %s is in pause state, message now put in retry store",
                    self.short_name
                )
            else:
                self.status = BaseChannel.PROCESSING
            await self.message_store.add_sub_message_state(
                id=msg.store_id, sub_id=msg.store_id, state=message.Message.PROCESSING)
            try:
                return await self._call_base_handling(msg=msg)
            except exceptions.RetryException as exc:
                self.logger.warning("Retry Exception caught: Set Channel in retry mode")
                self.status = BaseChannel.PAUSED
                raise exceptions.PausedChanException(exc)
            finally:
                self.processed_msgs += 1
                if self.status == BaseChannel.PROCESSING:
                    # Channel could be set in PAUSE state in the _call_base_handling,
                    # so make sure that the state don't change before re-WAITING message
                    self.status = BaseChannel.WAITING

    async def subhandle(self, msg, start_nodename=None):
        """ Overload this method only if you know what you are doing. Called by ``handle`` method.

        :param msg: To be processed msg.

        :return: Processed message
        """

        result = await self.process(msg, start_nodename=start_nodename)

        return result

    async def _process_nodes(self, nodes, msg, add_sub_state=True):
        """
        Pass the msg to a list of nodes

        Args:
            nodes (list of nodes.BaseNode objects): List of nodes to traverse in order
            msg (message.Message): Message to process
        """
        if add_sub_state:
            msg_store_id = msg.store_id
        else:
            msg_store_id = None
        msg_store = None
        msg_store_chan = msg.store_chan_name
        if msg_store_id and msg_store_chan:
            if msg_store_chan == self.short_name:
                msg_store = self.message_store
            else:
                msg_store = get_channel(name=msg_store_chan).message_store
        cur_res = msg
        for cur_node_idx, node in enumerate(nodes):
            if isinstance(cur_res, types.GeneratorType):
                # If the message is a generator, recursively call _process_nodes with yielded
                # sub messages
                gene = cur_res
                raised_drop = None
                raised_exc = None
                raised_retry_exc = None
                for gen_msg in gene:
                    try:
                        result = await self._process_nodes(nodes=nodes[cur_node_idx:], msg=gen_msg)
                    except exceptions.RetryException as exc:
                        raised_retry_exc = exc
                        break
                    except Dropped as exc:
                        raised_drop = exc
                    except Exception as exc:
                        raised_exc = exc
                if raised_retry_exc is not None:
                    if self.retry_store:
                        for gen_msg in gene:
                            # If a RetryException was raised by a yielded message before, store others
                            # in the retry store with an injection defined to the following node
                            # Reraise the retry exception at the end of the loop
                            await self.retry_store.store_until_retry(
                                msg=gen_msg,
                                nodename=nodes[cur_node_idx].name,
                            )
                    raise raised_retry_exc
                elif raised_exc is not None:
                    raise raised_exc
                elif raised_drop is not None:
                    raise raised_drop
                else:
                    # TODO: at moment the last message of the generator is returned,
                    # I do this to not break old behaviour, but I don't think it's a good
                    # approach
                    return result
            else:
                cur_msg_uuid = getattr(cur_res, "uuid", None)
                try:
                    res = await node.handle(cur_res.copy())
                except EndChanProcess:
                    logger.debug("EndChanProcess raised, ending channel process..")
                    if msg_store:
                        await msg_store.add_sub_message_state(
                            id=msg_store_id,
                            sub_id=cur_msg_uuid,
                            state=message.Message.PROCESSED,
                        )
                    return cur_res
                except Dropped as exc:
                    if msg_store:
                        await msg_store.add_sub_message_state(
                            id=msg_store_id,
                            sub_id=cur_msg_uuid,
                            state=message.Message.PROCESSED,
                        )
                    raise exc
                except Rejected as exc:
                    if msg_store:
                        await msg_store.add_sub_message_state(
                            id=msg_store_id,
                            sub_id=cur_msg_uuid,
                            state=message.Message.REJECTED,
                        )
                    raise exc
                except (exceptions.RetryException, exceptions.PausedChanException):
                    raise
                except Exception as exc:
                    if msg_store:
                        await msg_store.add_sub_message_state(
                            id=msg_store_id,
                            sub_id=cur_msg_uuid,
                            state=message.Message.ERROR,
                        )
                    raise exc
                cur_res = res
        if msg_store:
            await msg_store.add_sub_message_state(
                id=msg_store_id,
                sub_id=cur_msg_uuid,
                state=message.Message.PROCESSED,
            )
        return cur_res

    async def process(self, msg, start_nodename=None):
        """ Overload this method only if you know what you are doing. Called by ``subhandle`` method.

        :param msg: To be processed msg.

        :return: Processed message
        """

        if self._nodes:
            if start_nodename and start_nodename != "_initial":
                first_node = self.get_node(name=start_nodename)
                if first_node not in self._nodes:
                    raise Exception("Node %r not in self._nodes cannot inject", first_node)
            else:
                first_node = self._nodes[0]
            idx_first_node = self._nodes.index(first_node)
            nodes_to_process = self._nodes[idx_first_node:]
            res = await self._process_nodes(nodes=nodes_to_process, msg=msg)
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
            'short_name': self.short_name,
            'verbose_name': self.verbose_name,
            'status': BaseChannel.status_id_to_str(self.status),
            'has_message_store': self.has_message_store,
            'processed': self.processed_msgs,
            **self.extra_dict,
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

    def add_init_nodes(self, *nodes):
        """
        Add nodes that will be launched at the start of the channel before all
        processing nodes
        """
        if self.init_nodes:
            nodes = self.init_nodes.extend(nodes)
        self.init_nodes = self._init_end_nodes(*nodes)

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
            end_nodes = self.join_nodes.extend(end_nodes)
        self.join_nodes = self._init_end_nodes(*end_nodes)

    def add_fail_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises an Exception
        (except Dropped and Rejected)
        The first node take the entry message of the channel as input
        """
        if self.fail_nodes:
            end_nodes = self.fail_nodes.extend(end_nodes)
        self.fail_nodes = self._init_end_nodes(*end_nodes)

    def add_drop_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises a Dropped
        The first node take the entry message of the channel as input
        """
        if self.drop_nodes:
            end_nodes = self.drop_nodes.extend(end_nodes)
        self.drop_nodes = self._init_end_nodes(*end_nodes)

    def add_reject_nodes(self, *end_nodes):
        """
        Add nodes that will be launched only after a channel process that raises a Rejected
        The first node take the entry message of the channel as input
        """
        if self.reject_nodes:
            end_nodes = self.reject_nodes.extend(end_nodes)
        self.reject_nodes = self._init_end_nodes(*end_nodes)

    def add_final_nodes(self, *end_nodes):
        """
        Add nodes that will be launched all the time after a channel process
        The first node take the entry message of the channel as input (TODO: ask if it's ok)
        """
        if self.final_nodes:
            end_nodes = self.final_nodes.extend(end_nodes)
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
        retry_exc = None
        try:
            result = fut.result()
            entrymsg.chan_rslt = result
            if self.join_nodes:
                endnode_task = asyncio.create_task(self._call_special_nodes(result.copy(), node_type="join"))
                endnodes_tasks.append(endnode_task)
            logger.info(
                "Subchannel %s end process message %s, rslt is msg %s",
                str(self), str(entrymsg), str(result))
        except EndChanProcess:
            if self.join_nodes:
                endnode_task = asyncio.create_task(self._call_special_nodes(result.copy(), node_type="join"))
                endnodes_tasks.append(endnode_task)
            logger.info(
                "Subchannel %s end process message %s, rslt is msg %s",
                str(self), str(entrymsg), str(result))
        except Dropped as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.drop_nodes:
                endnode_task = asyncio.create_task(
                    self._call_special_nodes(entrymsg.copy(), node_type="drop"))
                endnodes_tasks.append(endnode_task)
            self.logger.info("Subchannel %s. Msg %s was dropped", str(self), str(entrymsg))
        except Rejected as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.reject_nodes:
                endnode_task = asyncio.create_task(
                    self._call_special_nodes(entrymsg.copy(), node_type="reject"))
                endnodes_tasks.append(endnode_task)
            self.logger.info("Subchannel %s. Msg %s was Rejected", str(self), str(entrymsg))
            raise
        except exceptions.RetryException as exc:
            retry_exc = exc
            raise exc
        except Exception as exc:
            entrymsg.chan_exc = exc
            entrymsg.chan_exc_traceback = traceback.format_exc()
            if self.fail_nodes:
                endnode_task = asyncio.create_task(
                    self._call_special_nodes(entrymsg.copy(), node_type="fail"))
                endnodes_tasks.append(endnode_task)
            self.logger.exception(
                "Error while processing msg %s in subchannel %s", str(entrymsg), str(self))
            raise
        finally:
            if self.final_nodes and not retry_exc:
                endnode_task = asyncio.create_task(
                    self._call_special_nodes(entrymsg.copy(), node_type="final"))
                endnodes_tasks.append(endnode_task)
            self.parent.sub_chan_endnodes.extend(endnodes_tasks)
            self.parent.sub_chan_tasks.remove(fut)
            self.logger.info(
                "subchan %s end process msg %s", str(self), str(entrymsg))

    async def subhandle(self, msg, start_nodename=None):
        msgctxvartoken = MSG_CTXVAR.set(msg.copy())
        ctx = contextvars.copy_context()
        copied_msg = msg.copy()
        if not self.has_message_store:
            # Reset the store id of the message to avoid adding sub messages status
            # of subchannel to the first message
            copied_msg.store_id = None
            copied_msg.store_chan_name = None
        fut = asyncio.create_task(self.process(copied_msg, start_nodename=start_nodename))
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

    async def handle(self, msg):
        if self.test_condition(msg):
            await super().handle(msg)
            raise EndChanProcess(f"cond subchan {self.short_name} ask to end parent")
        else:
            return msg


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
            # Lambda function in the following line permits dropping args and
            # kwargs as "handle" doesn't support other param than the msg and will be unuseful in
            # the MergeChannel's use case
            channel.subhandle = lambda msg, *args, **kwargs: self.handle(msg)

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
        asyncio.create_task(self.infinite_watcher())

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

    async def infinite_watcher(self):
        while not self.is_stopped():
            await self.check_and_process_folder()
            await self.interruptable_sleeper.sleep(self.interval)
        logger.info("Stopped watcher %s", self.short_name)

    async def watch_for_file(self):
        logger.warning(
            "FileWatcherChannel.watch_for_file func is deprecated and will "
            "be removed in future version"
        )
        await self.check_and_process_folder()

    async def check_and_process_folder(self):
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
                            await self.handle(msg)

        except Exception:  # TODO: might explicitely silence some special cases.
            self.logger.exception("filewatcher problem")


from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.hl7', 'MLLPChannel', ['hl7'])
wrap.add_lazy('pypeman.contrib.http', 'HttpChannel', ['aiohttp'])
wrap.add_lazy('pypeman.contrib.time', 'CronChannel', ['aiocron'])
wrap.add_lazy('pypeman.contrib.ftp', 'FTPWatcherChannel', [])
