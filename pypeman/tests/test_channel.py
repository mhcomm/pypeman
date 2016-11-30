import os
import unittest
import asyncio
import datetime
import shutil
import tempfile


from pypeman import channels
from pypeman.channels import BaseChannel
from pypeman import message
from pypeman import nodes
from pypeman import msgstore
from pypeman import events
from pypeman.tests.common import TestException

message_content = """{"test":1}"""


class TestNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used to test if node is processed during test

    def process(self, msg):
        print("Process %s" % self.name)
        return msg


class TestConditionalErrorNode(nodes.BaseNode):

    def process(self, msg):
        print("Process %s" % self.name)

        if msg.timestamp.day == 12:
            raise TestException()

        return msg


class ExceptNode(TestNode):
    # This node raise an exception
    def process(self, msg):
        result = super().process(msg)
        raise TestException()

def generate_msg(timestamp=(1981, 12, 28, 13, 37)):
    # Default message
    m = message.Message()
    m.timestamp = datetime.datetime(*timestamp)
    m.payload = message_content

    return m


class ChannelsTests(unittest.TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks
        pending = asyncio.Task.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending))

    def start_channels(self):
        # Start channels
        for chan in channels.all:
            self.loop.run_until_complete(chan.start())

    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

        # Avoid calling already tested channels
        channels.all.clear()

    def tearDown(self):
        self.clean_loop()

    def test_base_channel(self):
        """ Whether BaseChannel handling is working """

        chan = BaseChannel(name="test_channel1", loop=self.loop)
        n = TestNode()
        msg = generate_msg()

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_no_node_base_channel(self):
        """ Whether BaseChannel handling is working even if there is no node """

        chan = BaseChannel(name="test_channel2", loop=self.loop)
        msg = generate_msg()

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

    def test_sub_channel(self):
        """ Whether Sub Channel is working """

        chan = BaseChannel(name="test_channel3", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = TestNode(name="sub1")
        n4 = TestNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork(name="subchannel")
        sub.add(n2, n3, n4)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")
        self.assertTrue(n3.processed, "Sub Channel not working")
        self.assertTrue(n4.processed, "Sub Channel not working")
        self.assertEqual(sub.name, "test_channel3.subchannel", "Subchannel name is incorrect")

    def test_sub_channel_with_exception(self):
        """ Whether Sub Channel exception handling is working """

        chan = BaseChannel(name="test_channel4", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = ExceptNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork(name="Hello")
        sub.add(n2, n3)

        # Launch channel processing
        self.start_channels()

        self.loop.run_until_complete(chan.handle(msg))

        self.assertEqual(n1.processed, 1, "Sub Channel not working")

        with self.assertRaises(TestException) as cm:
            self.clean_loop()

        self.assertEqual(n2.processed, 1, "Sub Channel not working")

    def test_cond_channel(self):
        """ Whether Conditionnal channel is working """

        chan = BaseChannel(name="test_channel5", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")

        msg = generate_msg()

        chan.add(n1)

        # Nodes in this channel should not be processed
        cond1 = chan.when(lambda x: False, name="Toto")
        # Nodes in this channel should be processed
        cond2 = chan.when(True, name="condchannel")

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Cond Channel when condition == False not working")
        self.assertTrue(processed.processed, "Cond Channel when condition == True not working")
        self.assertFalse(n2.processed, "Cond Channel don't became the main path")
        self.assertEqual(cond2.name, "test_channel5.condchannel", "Condchannel name is incorrect")

    def test_case_channel(self):
        """ Whether Conditionnal channel is working """

        chan = BaseChannel(name="test_channel6", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")
        not_processed2 = TestNode(name="cond_proc2")

        msg = generate_msg()

        chan.add(n1)

        cond1, cond2, cond3 = chan.case(lambda x: False, True, True, names=['first', 'second', 'third'])

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)
        cond3.add(not_processed2)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Case Channel when condition == False not working")
        self.assertFalse(not_processed2.processed, "Case Channel when condition == False not working")
        self.assertTrue(processed.processed, "Case Channel when condition == True not working")
        self.assertTrue(n2.processed, "Cond Channel don't became the main path")
        self.assertEqual(cond1.name, "test_channel6.first", "Casechannel name is incorrect")
        self.assertEqual(cond2.name, "test_channel6.second", "Casechannel name is incorrect")
        self.assertEqual(cond3.name, "test_channel6.third", "Casechannel name is incorrect")

    def test_channel_result(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        @asyncio.coroutine
        def go():
            result = yield from chan.handle(msg)
            self.assertEqual(result.payload, msg.payload, "Channel handle not working")

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(go())

    def test_channel_events(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7.5", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        state_sequence = [chan.status]

        @events.channel_change_state.receiver
        def handle_change_state(channel=None, old_state=None, new_state=None):
            print(channel.name, old_state, new_state)
            state_sequence.append(new_state)

        @events.channel_change_state.receiver
        @asyncio.coroutine
        def handle_change_state_async(channel=None, old_state=None, new_state=None):
            print(channel.name, old_state, new_state)

        @asyncio.coroutine
        def go():
            result = yield from chan.handle(msg)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(go())
        self.loop.run_until_complete(chan.stop())

        print(state_sequence)

        valid_sequence = [BaseChannel.STOPPED, BaseChannel.STARTING, BaseChannel.WAITING,
                          BaseChannel.PROCESSING, BaseChannel.WAITING, BaseChannel.STOPPING, BaseChannel.STOPPED]
        self.assertEqual(state_sequence, valid_sequence, "Sequence state is not valid")

    def test_channel_stopped_dont_process_message(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7.7", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        @asyncio.coroutine
        def go():
            result = yield from chan.handle(msg)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(go())
        self.loop.run_until_complete(chan.stop())

        with self.assertRaises(channels.ChannelStopped) as cm:
            self.loop.run_until_complete(go())

    def test_channel_exception(self):
        """ Whether BaseChannel handling return an exception in case of error in main branch """

        chan = BaseChannel(name="test_channel8", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson(), ExceptNode())

        # Launch channel processing
        self.start_channels()
        with self.assertRaises(TestException) as cm:
            self.loop.run_until_complete(chan.process(msg))

    def test_null_message_store(self):
        """ We can store a message in NullMessageStore """

        chan = BaseChannel(name="test_channel9", loop=self.loop, message_store_factory=msgstore.FakeMessageStoreFactory())
        n = TestNode()
        msg = generate_msg()

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_memory_message_store(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()
        n_error = TestConditionalErrorNode()

        msg = generate_msg()
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35))
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35))

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35))

        chan.add(n)
        chan.add(n_error)

        # Launch channel processing

        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        with self.assertRaises(TestException) as cm:
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))

        msg_stored = list(chan.message_store.search())

        for msg in msg_stored:
            print(msg)

        # All message stored ?
        self.assertEqual(len(msg_stored), 5, "Should be 5 messages in store!")

        # Test processed message
        dict_msg = chan.message_store.get('%s' % msg3.uuid.hex)
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = chan.message_store.get('%s' % msg5.uuid.hex)
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

    def test_replay_from_memory_message_store(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10.5", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()

        msg = generate_msg()
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))


        chan.add(n)

        # Launch channel processing

        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))

        msg_stored = list(chan.message_store.search())

        for msg in msg_stored:
            print(msg)

        self.assertEqual(len(msg_stored), 2, "Should be 3 messages in store!")

        self.loop.run_until_complete(chan.replay(msg_stored[0]['id']))

        msg_stored = list(chan.message_store.search())
        self.assertEqual(len(msg_stored), 3, "Should be 3 messages in store!")

    def test_file_message_store(self):
        """ We can store a message in FileMessageStore """

        tempdir = tempfile.mkdtemp()

        store_factory = msgstore.FileMessageStoreFactory(path=tempdir)

        chan = BaseChannel(name="test_channel11", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()
        n_error = TestConditionalErrorNode()

        msg = generate_msg()
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35))
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35))

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35))

        chan.add(n)
        chan.add(n_error)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        with self.assertRaises(TestException) as cm:
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))

        msg_stored = list(chan.message_store.search())

        for msg in msg_stored:
            print(msg)

        # All message stored ?
        self.assertEqual(len(msg_stored), 5, "Should be 5 messages in store!")

        # Test processed message
        dict_msg = chan.message_store.get('1982/11/28/19821128_1235_%s' % msg3.uuid.hex)
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = chan.message_store.get('1982/11/12/19821112_1435_%s' % msg5.uuid.hex)
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

        self.assertTrue(os.path.exists("%s/%s/1982/11/28/19821128_1235_%s" % (tempdir, chan.name, msg3.uuid.hex)))

        # TODO put in tear_down ?
        shutil.rmtree(tempdir, ignore_errors=True)








