import asyncio

from pypeman import channels
from pypeman.channels import BaseChannel
from pypeman import nodes
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg


class TstNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TestingTests(TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks
        pending = asyncio.all_tasks(loop=self.loop)

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

    def test_chan_process_in_test_mode(self):
        """ Whether BaseChannel handling with test mode is working """

        chan = BaseChannel(name="test_channel_test_1", loop=self.loop)

        n = TstNode(name="testme")
        chan.add(n)

        msg = generate_msg(message_content="X")

        # Launch channel processing
        self.start_channels()
        chan._reset_test()

        chan.handle_and_wait(msg)

        self.assertEqual(n.processed, 1, "Channel in test mode not working")
        self.assertTrue(hasattr(n, '_handle'), "Channel in test mode not working")
        self.assertEqual(chan.get_node("testme").last_input().payload, "X", "Last input broken")

    def test_chan_node_mocking_in_test_mode(self):
        """ Whether node mocking input and output is working """

        chan = BaseChannel(name="test_channel_test_2", loop=self.loop)

        n = TstNode(name="testme")
        chan.add(n)

        msg_x = generate_msg(message_content="X")
        msg_a = generate_msg(message_content="A")
        msg_b = generate_msg(message_content="B")
        msg_c = generate_msg(message_content="C")
        msg_d = generate_msg(message_content="D")

        def concat_e(msg):
            msg.payload += "E"
            return msg

        def concat_f(msg):
            msg.payload += "F"
            return msg

        # Launch channel processing
        self.start_channels()

        # Mock input
        chan._reset_test()
        chan.get_node("testme").mock(input=msg_a)

        ret = chan.handle_and_wait(msg_x)

        self.assertEqual(n.processed, 1, "Channel in test mode not working")
        self.assertEqual(ret.payload, "A", "Mocking input broken")
        self.assertEqual(chan.get_node("testme").last_input().payload, "X", "Last input broken")

        # Mock output
        chan._reset_test()
        chan.get_node("testme").mock(output=msg_b)
        ret = chan.handle_and_wait(msg_x)

        self.assertEqual(n.processed, 1, "Channel in test mode not working")
        self.assertEqual(ret.payload, "B", "Mocking input broken")
        self.assertEqual(chan.get_node("testme").last_input().payload, "X", "Last input broken")

        # Mock both
        chan._reset_test()
        chan.get_node("testme").mock(input=msg_c, output=msg_d)
        ret = chan.handle_and_wait(msg_x)

        self.assertEqual(n.processed, 1, "Channel in test mode not working")
        self.assertEqual(ret.payload, "D", "Mocking both input and output broken")

        # Mock both functions
        chan._reset_test()
        chan.get_node("testme").mock(input=concat_e, output=concat_f)
        ret = chan.handle_and_wait(msg_x)

        self.assertEqual(n.processed, 1, "Channel in test mode not working")
        self.assertEqual(ret.payload, "XEF", "Mocking with function broken")
