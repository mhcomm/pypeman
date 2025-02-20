import asyncio
import copy
import logging

from pypeman import channels, endpoints
from pypeman import conf
from pypeman import nodes
from pypeman import msgstore
from pypeman import message
from pypeman.channels import BaseChannel, Rejected
from pypeman.retry import RetryFileMsgStore
from pypeman import exceptions
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg
from pypeman.tests.common import setup_settings
from pypeman.tests.common import teardown_settings


logger = logging.getLogger(__name__)

SETTINGS_MODULE = 'pypeman.tests.settings.test_settings_retry_store'


class TstExcNode(nodes.BaseNode):
    def __init__(self, *args, exc=KeyError, raise_exc=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.exc = exc
        self.raise_exc = raise_exc

    async def process(self, msg):
        if self.raise_exc:
            raise self.exc("Custom KeyError Exc raised")
        print(msg)
        return msg


class RetryStoreTests(TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks  # TODO: remove ?
        pending = asyncio.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending))
        self.loop.stop()
        self.loop.close()
        asyncio.set_event_loop(None)

    def start_channels(self):
        # Start channels
        for chan in channels.all_channels:
            self.loop.run_until_complete(chan.start())

    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(self.loop)

        # Avoid calling already tested channels
        channels.all_channels.clear()
        setup_settings(SETTINGS_MODULE)  # adapt config
        conf.settings.init_settings()

    def tearDown(self):
        super().tearDown()

        for end in endpoints.all_endpoints:
            self.loop.run_until_complete(end.stop())

        # Stop all channels
        for chan in channels.all_channels:
            if not chan.is_stopped():
                self.loop.run_until_complete(chan.stop())
        self.clean_loop()
        endpoints.reset_pypeman_endpoints()
        conf.settings.TEMPDIR.cleanup()
        teardown_settings()

    def _create_complete_retry_chan(self, base_chan_name="retry_chan"):
        """Create a complete chan to test retry store
        The chan graph looks like this:

        BaseChannel (name=retry_chan)
            |
        (init node, name=init_node) TstExcNode
            |
        (classic node) YielderNode
            |
        (classic node, name=first_node) TstExcNode
            |
            ---------------------------------------- FORK (name=retry_chan_forked)
            |                                                    |
            ---------WHEN (name=retry_chan_cond)      (classic node, name=forked_node) TstExcNode
            |                  |
            |     (classic node, name=conditional_node) TstExcNode
            |
        (classic node, name=last_node) TstExcNode
            |
        ---------------------------------
        |       |       |       |       |
        |       |       |       |   (join node, name=join_node) TstExcNode
        |       |       |   (drop node, name=drop_node) TstExcNode
        |       |   (reject node, name=reject_node) TstExcNode
        |   (fail node, name=fail_node) TstExcNode
        (final node, name=final_node) TstExcNode
        """

        base_chan = BaseChannel(
            name=base_chan_name,
            message_store_factory=msgstore.MemoryMessageStoreFactory(),
            loop=self.loop,
        )
        base_chan.add(
            nodes.YielderNode(),
            TstExcNode(
                name="first_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        forked_chan = base_chan.fork(name=f"{base_chan_name}_forked")
        forked_chan.add(
            TstExcNode(
                name="forked_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        conditional_chan = base_chan.when(lambda msg: msg.payload is True, name=f"{base_chan_name}_cond")
        conditional_chan.add(
            TstExcNode(
                name="conditional_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add(
            TstExcNode(
                name="last_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_init_nodes(
            TstExcNode(
                name="init_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_join_nodes(
            TstExcNode(
                name="join_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_drop_nodes(
            TstExcNode(
                name="drop_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_reject_nodes(
            TstExcNode(
                name="reject_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_fail_nodes(
            TstExcNode(
                name="fail_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        base_chan.add_final_nodes(
            TstExcNode(
                name="final_node",
                auto_retry_exceptions=[KeyError]
            ),
        )
        return base_chan, forked_chan, conditional_chan

    def test_complete_retry(self):
        """
        Test Retry store with complete_retry_chan

        The test follow these steps (message.payload=["msg1", "msg2", "msg3"]):
        - Test with all nodes that raise an error, init node catch the exception store it,
            put the main channel in Pause state, and set the message state in the "classic"
            msgstore to "wait_retry"
        - Retry without modifying nodes (same comportment)
        - Retry with init_node OK: first_node raise and catch the exception and the 3 yielded
            submessages are stored in chan retry_store
        - Retry with first_node OK: last_node raise and catch the exception and the 3 yielded
            submessages are stored in chan retry_store (and must be inject to first_node or
            last_node). forked node raise and catch an exception too, Put forked_chan in Pause
            state, and store the same sub message catched by last_node
        - Retry with forked_node OK: No changes in base channel and it's retry store, but no
        more pending message in forked chan's retry store and the chan is un-paused
        - Retry with last_node OK: The 3 messages are injected, the channel is un-paused and
            the message state in the msgstore is set to processed. End nodes are not called
        """
        chan, forked_chan, conditional_chan = self._create_complete_retry_chan(
            base_chan_name="comp_retry_chan")
        msgstore = chan.message_store
        retry_store = chan.retry_store
        forked_chan_retry_store = forked_chan.retry_store
        first_node = chan.get_node("first_node")
        forked_node = forked_chan.get_node("forked_node")
        last_node = chan.get_node("last_node")
        init_node = chan.get_node("init_node")

        msg_payload = ["msg1", "msg2", "msg3"]
        msg_meta = {"titi": "toto"}
        msg = generate_msg(message_content=msg_payload, message_meta=msg_meta)
        chan._reset_test()
        self.start_channels()
        print("START CHANNEL")

        print("\n")
        print("CHANNEL HANDLE 1")
        # Test with all nodes that raise an error, init node catch the exception store it
        # Assert:
        # - the main channel is in Pause state
        # - the chan retry_store is started and in retry mode
        # - The stored message state is "wait_retry"
        # - one message in the retry store
        # - The only message to retry correspond to base message and have to be inject in
        #       init_node
        with self.assertRaises(exceptions.PausedChanException):
            self.loop.run_until_complete(chan.handle(msg))
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 1
        assert stored_msg["state"] == message.Message.WAIT_RETRY
        assert msgs_retry_store[0]["message"].payload == stored_msg["message"].payload
        assert msgs_retry_store[0]["message"].meta == stored_msg["message"].meta
        assert msgs_retry_store[0]["meta"]["store_id"] == stored_msg["id"]
        assert msgs_retry_store[0]["meta"]["store_chan_name"] == chan.short_name
        assert msgs_retry_store[0]["meta"]["nodename"] == init_node.name
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING

        print("\n")
        print("RETRY")
        # Retry without modifying nodes (same comportment as above, no modification)
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 1
        assert msgs_retry_store[0]["message"].payload == stored_msg["message"].payload
        assert msgs_retry_store[0]["message"].meta == stored_msg["message"].meta
        assert msgs_retry_store[0]["meta"]["store_id"] == stored_msg["id"]
        assert msgs_retry_store[0]["meta"]["store_chan_name"] == chan.short_name
        assert msgs_retry_store[0]["meta"]["nodename"] == init_node.name
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("RETRY2 init node OK")
        # Retry with init_node OK: first_node raise and catch the exception
        # Assert:
        # - the main chan is already paused
        # - The 3 yielded submessages are stored in chan retry_store
        # - They all have to be inject in first_node
        # - Their payloads corresponds to element in input message's payload list
        init_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 3
        copied_msg_payload = copy.deepcopy(msg_payload)
        for msg_to_retry in msgs_retry_store:
            retry_msg_payload = msg_to_retry["message"].payload
            copied_msg_payload.remove(retry_msg_payload)
            assert msg_to_retry["message"].meta == stored_msg["message"].meta
            assert msg_to_retry["meta"]["store_id"] == stored_msg["id"]
            assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
            assert msg_to_retry["meta"]["nodename"] == first_node.name
        assert len(copied_msg_payload) == 0
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("RETRY3 first node OK")
        # Retry with first_node OK: last_node raise and catch the exception,
        # forked_node do the same
        # Assert for the main chan:
        # - the chan is already paused
        # - The 3 yielded submessages are stored in chan retry_store
        # - One of the 3 messages have to be inject in last_node, others in first_node
        # - Their payloads corresponds to element in input message's payload list
        # Assert for the forked chan:
        # - the chan is in pause state
        # - the chan retry_store is started and in retry mode
        # - One submessage in the retry store, and must be inject in forked_node
        # - The message's payload must correspond to the one catch by last_node

        first_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 3
        copied_msg_payload = copy.deepcopy(msg_payload)
        found_msg = None
        for msg_to_retry in msgs_retry_store:
            retry_msg_payload = msg_to_retry["message"].payload
            copied_msg_payload.remove(retry_msg_payload)
            nodename_where_inject = msg_to_retry["meta"]["nodename"]
            if nodename_where_inject == first_node.name:
                continue
            assert nodename_where_inject == last_node.name
            found_msg = msg_to_retry
            assert msg_to_retry["message"].meta == stored_msg["message"].meta
            assert msg_to_retry["meta"]["store_id"] == stored_msg["id"]
            assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
        assert len(copied_msg_payload) == 0
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.PAUSED
        assert forked_chan_retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert conditional_chan.status == BaseChannel.WAITING
        cnt_msgs_retrystore = self.loop.run_until_complete(forked_chan_retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(forked_chan_retry_store.search(order_by="timestamp"))
        assert len(msgs_retry_store) == 1
        msg_retry_store = msgs_retry_store[0]
        assert msg_retry_store["message"].meta == found_msg["message"].meta
        assert msg_retry_store["message"].payload == found_msg["message"].payload
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("RETRY4.1 forked node OK")
        # Retry with forked node OK: last_node raise and catch the exception,
        # forked chan now working correctly
        # Assert for the main chan:
        # - the chan is already paused
        # - The 3 yielded submessages are stored in chan retry_store
        # - The 3 messages have to be inject in last_node or in first_node
        # - Their payloads corresponds to element in input message's payload list
        # Assert for the forked chan:
        # - the chan is started
        # - the chan retry_store is stopped
        # - No more message in the retry store
        forked_node.raise_exc = False
        # First unblock the forked chan
        self.loop.run_until_complete(forked_chan_retry_store.retry())
        # Then relaunch other messages
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 3
        copied_msg_payload = copy.deepcopy(msg_payload)
        found_msg = None
        for msg_to_retry in msgs_retry_store:
            retry_msg_payload = msg_to_retry["message"].payload
            copied_msg_payload.remove(retry_msg_payload)
            nodename_where_inject = msg_to_retry["meta"]["nodename"]
            if nodename_where_inject == first_node.name:
                continue
            assert nodename_where_inject == last_node.name
            found_msg = msg_to_retry
            assert msg_to_retry["message"].meta == stored_msg["message"].meta
            assert msg_to_retry["meta"]["store_id"] == stored_msg["id"]
            assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
        assert len(copied_msg_payload) == 0
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert forked_chan_retry_store.state == RetryFileMsgStore.STOPPED
        assert conditional_chan.status == BaseChannel.WAITING
        cnt_msgs_retrystore = self.loop.run_until_complete(forked_chan_retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(forked_chan_retry_store.search(order_by="timestamp"))
        assert len(msgs_retry_store) == 0
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("RETRY5 last node OK")
        # Retry with last_node OK: join_node raise and catch the exception
        # Assert for the main chan:
        # - the chan is already paused
        # - Only one message in chan retry_store
        # - The message have to be inject in join_node
        last_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 0
        assert chan.status == BaseChannel.WAITING
        assert retry_store.state == RetryFileMsgStore.STOPPED
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.PROCESSED

    def test_dual_msgs_n_endnodes(self):
        """
        Test Retry store with 2 messages, only first_node, join_node and final nodes raise exceptions
        (that are catched)
        first msg must be inject in first_node,
        and second in channel start point.
        First will not call endnodes at retry, second yes.

        message.payload=["msg1", "msg2", "msg3"]

        assert that:
        - messages are processed in order by retry store
        - only messages that have to be inject from start go to end nodes
        - retry works with join and final nodes
        """
        chan, forked_chan, conditional_chan = self._create_complete_retry_chan(
            base_chan_name="2end_retry_chan")
        msgstore = chan.message_store
        retry_store = chan.retry_store
        forked_chan_retry_store = forked_chan.retry_store
        first_node = chan.get_node("first_node")
        forked_node = forked_chan.get_node("forked_node")
        forked_node.raise_exc = False
        last_node = chan.get_node("last_node")
        last_node.raise_exc = False
        init_node = chan.get_node("init_node")
        init_node.raise_exc = False
        join_node = chan.get_node("join_node")
        drop_node = chan.get_node("drop_node")
        drop_node.raise_exc = False
        reject_node = chan.get_node("reject_node")
        reject_node.raise_exc = False
        fail_node = chan.get_node("fail_node")
        fail_node.raise_exc = False
        final_node = chan.get_node("final_node")

        msg_payload = ["msg1", "msg2", "msg3"]
        msg_meta = {"titi": "toto"}
        msg1 = generate_msg(message_content=msg_payload, message_meta=msg_meta)
        msg2 = generate_msg(message_content=msg_payload, message_meta=msg_meta)
        chan._reset_test()
        self.start_channels()

        print("First Handle")
        # Handle the 2 messages, the first raise a retry exception at first_node, the second
        # is automatically put in queue at start
        #
        # Assert:
        # - That the chan is in pause state
        # - That the first message have 3 sub messages in retry store that must
        #   be inject in first_node
        # - That the second message is in retry store and must be inject at startpoint
        with self.assertRaises(exceptions.PausedChanException):
            self.loop.run_until_complete(chan.handle(msg1))
        stored_msg1 = self.loop.run_until_complete(msgstore.get(id=msg1.uuid))
        with self.assertRaises(exceptions.PausedChanException):
            self.loop.run_until_complete(chan.handle(msg2))
        stored_msg2 = self.loop.run_until_complete(msgstore.get(id=msg2.uuid))
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 4
        assert stored_msg1["state"] == message.Message.WAIT_RETRY
        assert stored_msg2["state"] == message.Message.WAIT_RETRY
        copied_msg_payload = copy.deepcopy(msg_payload)
        for msg_to_retry in msgs_retry_store:
            store_id = msg_to_retry["meta"]["store_id"]
            if store_id == stored_msg1["id"]:
                retry_msg_payload = msg_to_retry["message"].payload
                copied_msg_payload.remove(retry_msg_payload)
                assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
                assert msg_to_retry["meta"]["nodename"] == first_node.name
            elif store_id == stored_msg2["id"]:
                assert msg_to_retry["message"].payload == msg_payload
                assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
                assert msg_to_retry["meta"]["nodename"] is None
            else:
                raise Exception("Msg to retry store_id %s not in store", store_id)
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE

        print("\n")
        print("RETRY first node OK")
        # Retry with first node OK (but endnodes KO)
        #
        # Assert:
        # - That the chan is in pause state
        # - That the 3 submessages of msg 1 passed and set the msg1 state to OK
        # - That the second message pass in join nodes, raise a Retry exception,
        #   is in wait_retry state and must be inject in join_node
        first_node.raise_exc = False
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 1
        assert msgs_retry_store[0]["message"].payload == msg_payload[-1]
        assert msgs_retry_store[0]["message"].meta == stored_msg2["message"].meta
        assert msgs_retry_store[0]["meta"]["store_id"] == stored_msg2["id"]
        assert msgs_retry_store[0]["meta"]["store_chan_name"] == chan.short_name
        assert msgs_retry_store[0]["meta"]["nodename"] == join_node.name
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert forked_chan_retry_store.state == RetryFileMsgStore.STOPPED
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg1 = self.loop.run_until_complete(msgstore.get(id=msg1.uuid))
        stored_msg2 = self.loop.run_until_complete(msgstore.get(id=msg2.uuid))
        assert stored_msg1["state"] == message.Message.PROCESSED
        assert stored_msg2["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("RETRY join node OK")
        # Retry with join_node node OK
        #
        # Assert:
        # - The chan is in pause state
        # - The second message pass join nodes, but raise a Retry exception in final_node,
        #   is in wait_retry state and must be inject in final_node
        join_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 1
        assert msgs_retry_store[0]["message"].payload == msg_payload
        assert msgs_retry_store[0]["message"].meta == stored_msg2["message"].meta
        assert msgs_retry_store[0]["meta"]["store_id"] == stored_msg2["id"]
        assert msgs_retry_store[0]["meta"]["store_chan_name"] == chan.short_name
        assert msgs_retry_store[0]["meta"]["nodename"] == final_node.name
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert forked_chan_retry_store.state == RetryFileMsgStore.STOPPED
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg1 = self.loop.run_until_complete(msgstore.get(id=msg1.uuid))
        stored_msg2 = self.loop.run_until_complete(msgstore.get(id=msg2.uuid))
        assert stored_msg1["state"] == message.Message.PROCESSED
        assert stored_msg2["state"] == message.Message.PROCESSED

        print("\n")
        print("RETRY final node OK")
        # Retry with final_node node OK
        #
        # Assert:
        # - The chan is in WAITING state
        # - The retry store is stopped
        # - The second message is processed, and state is set
        final_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 0
        assert chan.status == BaseChannel.WAITING
        assert retry_store.state == RetryFileMsgStore.STOPPED
        assert forked_chan.status == BaseChannel.WAITING
        assert forked_chan_retry_store.state == RetryFileMsgStore.STOPPED
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg1 = self.loop.run_until_complete(msgstore.get(id=msg1.uuid))
        stored_msg2 = self.loop.run_until_complete(msgstore.get(id=msg2.uuid))
        assert stored_msg1["state"] == message.Message.PROCESSED
        assert stored_msg2["state"] == message.Message.PROCESSED

    def test_rejected_in_when(self):
        """
        Test Retry store with 1 message, there's only one retry exception raise in the conditional subchan
        One submessage go to the subchan
        When the retry is done, conditional subchan raise a Rejected

        message.payload=["msg1", True, "msg3"]

        assert that:
        - The conditional subchan AND the main chan are in PAUSED state
            while the retry is not done
        - The base message is in WAIT_RETRY
        - Once retries are done, the base message is in Rejected status
        """
        chan, forked_chan, conditional_chan = self._create_complete_retry_chan(
            base_chan_name="tstcond_retry_chan")
        msgstore = chan.message_store
        retry_store = chan.retry_store
        cond_chan_retry_store = conditional_chan.retry_store
        first_node = chan.get_node("first_node")
        first_node.raise_exc = False
        forked_node = forked_chan.get_node("forked_node")
        forked_node.raise_exc = False
        conditional_node = conditional_chan.get_node("conditional_node")
        last_node = chan.get_node("last_node")
        last_node.raise_exc = False
        init_node = chan.get_node("init_node")
        init_node.raise_exc = False
        join_node = chan.get_node("join_node")
        join_node.raise_exc = False
        drop_node = chan.get_node("drop_node")
        drop_node.raise_exc = False
        reject_node = chan.get_node("reject_node")
        reject_node.raise_exc = False
        fail_node = chan.get_node("fail_node")
        fail_node.raise_exc = False
        final_node = chan.get_node("final_node")
        final_node.raise_exc = False

        msg_payload = ["msg1", True, "msg3"]
        msg_meta = {"titi": "toto"}
        msg = generate_msg(message_content=msg_payload, message_meta=msg_meta)
        chan._reset_test()
        self.start_channels()

        print("\n")
        print("CHANNEL HANDLE 1")
        # Test with the retry exception in the conditional subchan
        # Assert:
        # - The message is in wait_retry state
        # - The conditional subchan is in PAUSED state
        # - The retry store of the conditional subchan is started
        # - The "True" submessage is in the retry store
        with self.assertRaises(exceptions.PausedChanException):
            self.loop.run_until_complete(chan.handle(msg))
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY
        cnt_msgs_cond_retrystore = self.loop.run_until_complete(cond_chan_retry_store.count_msgs())
        assert cnt_msgs_cond_retrystore == 1
        msg_cond_retry_store = self.loop.run_until_complete(
            cond_chan_retry_store.search(order_by="timestamp"))[0]
        assert msg_cond_retry_store["message"].payload is True
        assert msg_cond_retry_store["message"].meta == stored_msg["message"].meta
        assert msg_cond_retry_store["meta"]["store_id"] == stored_msg["id"]
        assert msg_cond_retry_store["meta"]["store_chan_name"] == chan.short_name
        assert msg_cond_retry_store["meta"]["nodename"] == conditional_node.name
        assert chan.status == BaseChannel.WAITING
        assert retry_store.state == RetryFileMsgStore.STOPPED
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.PAUSED
        assert cond_chan_retry_store.state == RetryFileMsgStore.RETRY_MODE

        print("\n")
        print("RETRY conditional node raise Rejected")
        # Retry with the a Rejected instead of RetryException in the conditional subchan
        # Assert:
        # - The message is in REJECTED state
        # - The conditional subchan is in WAITING state
        # - The retry store of the conditional subchan is stopped
        conditional_node.exc = Rejected
        self.loop.run_until_complete(cond_chan_retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(cond_chan_retry_store.count_msgs())
        assert cnt_msgs_retrystore == 0
        assert chan.status == BaseChannel.WAITING
        assert retry_store.state == RetryFileMsgStore.STOPPED
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        assert cond_chan_retry_store.state == RetryFileMsgStore.STOPPED
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.REJECTED

    def test_error_in_yielded_msg(self):
        """
        Test Retry store with 1 message, there's only one retry exception raised
        in the first node
        The last_node raise an Exception

        message.payload=["msg1", True, "msg3"]

        assert that:
        - The main chan is in PAUSED state while the retry is not done
        - The base message is in WAIT_RETRY
        - Once retries are done, the base message is in Error status
        """
        chan, forked_chan, conditional_chan = self._create_complete_retry_chan(
            base_chan_name="tstcond_w_err_retry_chan")
        msgstore = chan.message_store
        retry_store = chan.retry_store
        first_node = chan.get_node("first_node")
        forked_node = forked_chan.get_node("forked_node")
        forked_node.raise_exc = False
        conditional_node = conditional_chan.get_node("conditional_node")
        conditional_node.raise_exc = False
        last_node = chan.get_node("last_node")
        last_node.exc = Exception
        init_node = chan.get_node("init_node")
        init_node.raise_exc = False
        join_node = chan.get_node("join_node")
        join_node.raise_exc = False
        drop_node = chan.get_node("drop_node")
        drop_node.raise_exc = False
        reject_node = chan.get_node("reject_node")
        reject_node.raise_exc = False
        fail_node = chan.get_node("fail_node")
        fail_node.raise_exc = False
        final_node = chan.get_node("final_node")
        final_node.raise_exc = False

        msg_payload = ["msg1", True, "msg3"]
        msg_meta = {"titi": "toto"}
        msg = generate_msg(message_content=msg_payload, message_meta=msg_meta)
        chan._reset_test()
        self.start_channels()

        print("\n")
        print("CHANNEL HANDLE 1")
        # Test with first node that raise a retry exception
        # Assert:
        # - the main chan is paused
        # - The 3 yielded submessages are stored in chan retry_store
        # - They all have to be inject in first_node
        # - Their payloads corresponds to element in input message's payload list
        with self.assertRaises(exceptions.PausedChanException):
            self.loop.run_until_complete(chan.handle(msg))
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        msgs_retry_store = self.loop.run_until_complete(retry_store.search(order_by="timestamp"))
        assert cnt_msgs_retrystore == 3
        copied_msg_payload = copy.deepcopy(msg_payload)
        for msg_to_retry in msgs_retry_store:
            retry_msg_payload = msg_to_retry["message"].payload
            copied_msg_payload.remove(retry_msg_payload)
            assert msg_to_retry["message"].meta == stored_msg["message"].meta
            assert msg_to_retry["meta"]["store_id"] == stored_msg["id"]
            assert msg_to_retry["meta"]["store_chan_name"] == chan.short_name
            assert msg_to_retry["meta"]["nodename"] == first_node.name
        assert len(copied_msg_payload) == 0
        assert chan.status == BaseChannel.PAUSED
        assert retry_store.state == RetryFileMsgStore.RETRY_MODE
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.WAIT_RETRY

        print("\n")
        print("CHANNEL Retry")
        # Test with all nodes OK (but last node that raise a "classic" exception)
        # Assert:
        # - the main chan is started
        # - The retry store is stopped
        # - The message state is ERROR
        first_node.raise_exc = False
        self.loop.run_until_complete(retry_store.retry())
        cnt_msgs_retrystore = self.loop.run_until_complete(retry_store.count_msgs())
        assert cnt_msgs_retrystore == 0
        assert chan.status == BaseChannel.WAITING
        assert retry_store.state == RetryFileMsgStore.STOPPED
        assert forked_chan.status == BaseChannel.WAITING
        assert conditional_chan.status == BaseChannel.WAITING
        stored_msg = self.loop.run_until_complete(msgstore.get(id=msg.uuid))
        assert stored_msg["state"] == message.Message.ERROR
