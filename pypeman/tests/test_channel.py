import unittest
import asyncio

from pypeman.channels import BaseChannel
from pypeman import message
from pypeman import nodes

message_content = """{"test":1}"""

class TestNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used tu test if node is processed during test
        self.processed = False

    def process(self, msg):
        print("Process %s" % self.name)
        self.processed = True
        return msg


class ExceptNode(nodes.BaseNode):
    # This node raise an exception
    def process(self, msg):
        raise Exception()


def generate_msg():
    # Default message
    m = message.Message()
    m.payload = message_content

    return m


class ChannelsTests(unittest.TestCase):
    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not shure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

    def test_base_channel(self):
        """ if BaseChannel handling is working """

        chan = BaseChannel(loop=self.loop)
        n = TestNode()
        msg = generate_msg()

        chan.add(n)

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_no_node_base_channel(self):
        """ if BaseChannel handling is working even if there is no node """

        chan = BaseChannel(loop=self.loop)
        msg = generate_msg()

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))


    def test_sub_channel(self):
        """ if Sub Channel is working """

        chan = BaseChannel(loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork()
        sub.add(n2)

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")

    def test_cond_channel(self):
        """ if Conditionnal channel is working """

        chan = BaseChannel(loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")

        msg = generate_msg()

        chan.add(n1)

        # Nodes in this channel should not be processed
        cond1 = chan.when(lambda x: False)
        # Nodes in this channel should be processed
        cond2 = chan.when(lambda x: True)

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Cond Channel when condition == False not working")
        self.assertTrue(processed.processed, "Cond Channel when condition == True not working")
        self.assertFalse(n2.processed, "Cond Channel don't became the main path")

    def test_channel_result(self):
        """ if BaseChannel handling return a good result """

        chan = BaseChannel(loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        @asyncio.coroutine
        def go():
            result = yield from chan.process(msg)
            self.assertEqual(result.payload, msg.payload, "Channel handle not working")

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(go())


    def test_channel_exception(self):
        """ if BaseChannel handling return an exception in case of error in main branch """

        chan = BaseChannel(loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson(), ExceptNode())

        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        with self.assertRaises(Exception) as cm:
            self.loop.run_until_complete(chan.process(msg))







