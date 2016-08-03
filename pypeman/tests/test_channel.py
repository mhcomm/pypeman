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
from pypeman.tests.common import TestException

message_content = """{"test":1}"""

class TestNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used to test if node is processed during test
        self.processed = False

    def process(self, msg):
        print("Process %s" % self.name)
        self.processed = True
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
        self.loop.run_until_complete(asyncio.gather(*pending))

        self.loop.close()

    def start_channels(self):
        # Start channels
        for chan in channels.all:
            chan.import_modules()
            self.loop.run_until_complete(chan.start())

    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not shure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

    def test_base_channel(self):
        """ if BaseChannel handling is working """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        n = TestNode()
        msg = generate_msg()

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_no_node_base_channel(self):
        """ if BaseChannel handling is working even if there is no node """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        msg = generate_msg()

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

    def test_sub_channel(self):
        """ if Sub Channel is working """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = TestNode(name="sub1")
        n4 = TestNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork()
        sub.add(n2, n3, n4)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")
        self.assertTrue(n3.processed, "Sub Channel not working")
        self.assertTrue(n4.processed, "Sub Channel not working")

    def test_sub_channel_with_exception(self):
        """ if Sub Channel exception handling is working """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = ExceptNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork()
        sub.add(n2, n3)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")

        with self.assertRaises(TestException) as cm:
            self.clean_loop()

    def test_cond_channel(self):
        """ if Conditionnal channel is working """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")

        msg = generate_msg()

        chan.add(n1)

        # Nodes in this channel should not be processed
        cond1 = chan.when(lambda x: False)
        # Nodes in this channel should be processed
        cond2 = chan.when(True)

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Cond Channel when condition == False not working")
        self.assertTrue(processed.processed, "Cond Channel when condition == True not working")
        self.assertFalse(n2.processed, "Cond Channel don't became the main path")

    def test_case_channel(self):
        """ if Conditionnal channel is working """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")
        not_processed2 = TestNode(name="cond_proc2")

        msg = generate_msg()

        chan.add(n1)

        cond1, cond2, cond3 = chan.case(lambda x: False, True, True)

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

    def test_channel_result(self):
        """ if BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        @asyncio.coroutine
        def go():
            result = yield from chan.process(msg)
            self.assertEqual(result.payload, msg.payload, "Channel handle not working")

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(go())

    def test_channel_exception(self):
        """ if BaseChannel handling return an exception in case of error in main branch """

        chan = BaseChannel(name="test_channel", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson(), ExceptNode())

        # Launch channel processing
        self.start_channels()
        with self.assertRaises(TestException) as cm:
            self.loop.run_until_complete(chan.process(msg))

    def test_null_message_store(self):
        """ We can store a message in NullMessageStore """

        chan = BaseChannel(name="test_channel", loop=self.loop, message_store_factory=msgstore.FakeMessageStoreFactory())
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

        chan = BaseChannel(name="test_channel", loop=self.loop, message_store_factory=store_factory)

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

        try:
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))
        except TestException as e:
            pass

        msg_stored = list(chan.message_store.search(chan.name))

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


    def test_file_message_store(self):
        """ We can store a message in FileMessageStore """

        tempdir = tempfile.mkdtemp()

        store_factory = msgstore.FileMessageStoreFactory(path=tempdir)

        chan = BaseChannel(name="test_channel", loop=self.loop, message_store_factory=store_factory)

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

        try:
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))
        except TestException as e:
            pass

        msg_stored = list(chan.message_store.search(chan.name))

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


        # TODO put in tear_down ?
        shutil.rmtree(tempdir, ignore_errors=True)








