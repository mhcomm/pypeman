import os
import asyncio
import shutil
import tempfile

from pypeman import channels
from pypeman import msgstore
from pypeman import nodes
from pypeman.channels import BaseChannel
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TestException
from pypeman.tests.common import TestNode


class TestConditionalErrorNode(nodes.BaseNode):

    def process(self, msg):
        print("Process %s" % self.name)

        if "shall_fail" in msg.payload:
            raise TestException()

        return msg


class MsgstoreTests(TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks
        pending = asyncio.Task.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending))

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
        asyncio.set_event_loop(None)

        # Avoid calling already tested channels
        channels.all_channels.clear()

    def tearDown(self):
        super().tearDown()
        self.clean_loop()

    def test_null_message_store(self):
        """ We can store a message in NullMessageStore """

        chan = BaseChannel(
            name="test_channel9", loop=self.loop,
            message_store_factory=msgstore.FakeMessageStoreFactory())
        n = TestNode()
        msg = generate_msg(with_context=True)

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

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35))
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35))

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35),
                            message_content='{"test1": "shall_fail"}')

        chan.add(n)
        chan.add(n_error)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        with self.assertRaises(TestException):
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))

        self.assertEqual(self.loop.run_until_complete(chan.message_store.total()),
                         5, "Should be a total of 5 messages in store!")

        msg_stored1 = list(self.loop.run_until_complete(chan.message_store.search(0, 2)))
        self.assertEqual(len(msg_stored1), 2, "Should be 2 results from search!")

        msg_stored2 = list(self.loop.run_until_complete(chan.message_store.search(2, 5)))
        self.assertEqual(len(msg_stored2), 3, "Should be 3 results from search!")

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))

        # All message stored ?
        self.assertEqual(len(msg_stored), 5, "Should be 5 messages in store!")

        for msg in msg_stored:
            print(msg)

        for msg in msg_stored1 + msg_stored2:
            print(msg)

        ids1 = [msg['id'] for msg in msg_stored1 + msg_stored2]
        ids2 = [msg['id'] for msg in msg_stored]

        self.assertEqual(ids1, ids2, "Should be 5 messages in store!")

        # Test processed message
        dict_msg = self.loop.run_until_complete(chan.message_store.get('%s' % msg3.uuid))
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = self.loop.run_until_complete(chan.message_store.get('%s' % msg5.uuid))
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

    def test_memory_message_store_in_fork(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10.25", loop=self.loop, message_store_factory=store_factory)

        n1 = TestNode()
        n2 = TestNode()
        n3 = TestNode()
        n4 = TestNode()

        chan.add(n1, n2)
        fork = chan.fork()
        fork.add(n3)

        self.assertTrue(isinstance(fork.message_store, msgstore.NullMessageStore))

        whe = chan.when(True, message_store_factory=store_factory)
        whe.add(n4)

        self.assertTrue(isinstance(whe.message_store, msgstore.MemoryMessageStore))

    def test_replay_from_memory_message_store(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10.5", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))

        for msg in msg_stored:
            print(msg)

        self.assertEqual(len(msg_stored), 2, "Should be 2 messages in store!")

        self.loop.run_until_complete(chan.replay(msg_stored[0]['id']))

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))
        self.assertEqual(len(msg_stored), 3, "Should be 3 messages in store!")

    def test_file_message_store(self):
        """ We can store a message in FileMessageStore """

        tempdir = tempfile.mkdtemp()
        print(tempdir)

        store_factory = msgstore.FileMessageStoreFactory(path=tempdir)

        chan = BaseChannel(name="test_channel11", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()
        n_error = TestConditionalErrorNode()

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35))
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35))

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35),
                            message_content='{"test1": "shall_fail"}')

        chan.add(n)
        chan.add(n_error)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        with self.assertRaises(TestException):
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))

        self.assertEqual(self.loop.run_until_complete(chan.message_store.total()),
                         5, "Should be a total of 5 messages in store!")

        msg_stored1 = list(self.loop.run_until_complete(chan.message_store.search(0, 2)))
        self.assertEqual(len(msg_stored1), 2, "Should be 2 results from search!")

        msg_stored2 = list(self.loop.run_until_complete(chan.message_store.search(2, 5)))
        self.assertEqual(len(msg_stored2), 3, "Should be 3 results from search!")

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))

        # All message stored ?
        self.assertEqual(len(msg_stored), 5, "Should be 5 messages in store!")

        for msg in msg_stored:
            print(msg)

        for msg in msg_stored1 + msg_stored2:
            print(msg)

        ids1 = [msg['id'] for msg in msg_stored1 + msg_stored2]
        ids2 = [msg['id'] for msg in msg_stored]

        self.assertEqual(ids1, ids2, "Should be 5 messages in store!")

        # Test processed message
        dict_msg = self.loop.run_until_complete(
            chan.message_store.get('1982/11/28/19821128_1235_%s' % msg3.uuid))
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = self.loop.run_until_complete(
            chan.message_store.get('1982/11/12/19821112_1435_%s' % msg5.uuid))
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

        self.assertTrue(os.path.exists("%s/%s/1982/11/28/19821128_1235_%s"
                        % (tempdir, chan.name, msg3.uuid)))

        self.clean_loop()

        # TODO put in tear_down ?
        shutil.rmtree(tempdir, ignore_errors=True)
