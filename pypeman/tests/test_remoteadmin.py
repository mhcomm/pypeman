import asyncio

import pytest
import pytest_asyncio.plugin  # noqa F401

from pypeman import nodes, msgstore, channels
from pypeman.channels import BaseChannel
from pypeman.remoteadmin import RemoteAdminClient, RemoteAdminServer
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg


class TestNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used to test if node is processed during test

    def process(self, msg):
        print("Process %s" % self.name)
        return msg


class RemoteAdminTests(TestCase):

    @pytest.fixture(autouse=True)
    def initfixture(self, unused_tcp_port):
        self.tcp_port = unused_tcp_port

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

    def test_remote_admin_list(self):
        """ Channel remote listing working """

        port = self.tcp_port  # port used for rmt admin

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_remote050", loop=self.loop, message_store_factory=store_factory)

        n = TestNode()
        n2 = TestNode(name="sub")
        n3 = TestNode(name="sub1")
        n4 = TestNode(name="sub2")

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35))
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35))

        idref_msg3 = msg3.uuid

        chan.add(n)

        sub = chan.fork(name="subchannel")
        sub.append(n2, n3, n4)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        server = RemoteAdminServer(loop=self.loop, port=port)

        self.loop.run_until_complete(server.start())

        client = RemoteAdminClient(loop=self.loop, url="ws://localhost:%d" % port)
        client.init()

        # List channels
        chans = client.channels()

        print(chans)

        self.assertEqual(chans[0]['name'], 'test_remote050', "Channel listing not working")

        self.assertEqual(
            chans[0]['subchannels'][0]['name'],
            'test_remote050.subchannel',
            "Subchannel listing not working")

        # Stop channel
        result = client.stop('test_remote050')

        self.assertEqual(chan.status, BaseChannel.STOPPED, "Stopping channel doesn't work")

        # Start channel
        result = client.start('test_remote050')

        self.assertEqual(chan.status, BaseChannel.WAITING, "Starting channel doesn't work")

        # Search message
        msg_list = client.list_msg(channel='test_remote050', start=2, count=5, order_by='-timestamp')

        print(msg_list)

        self.assertEqual(msg_list['total'], 4, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], idref_msg3, 'List channel messages broken')

        # Replay message
        result = client.replay_msg('test_remote050', [idref_msg3])

        msg_list = client.list_msg(channel='test_remote050', start=0, count=5, order_by='-timestamp')
        self.assertEqual(msg_list['total'], 5, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], result[0].uuid, 'Replay messages broken')

        # Push message
        result = client.push_msg(channel='test_remote050', text="Yaaay")

        msg_list = client.list_msg(channel='test_remote050', start=0, count=5, order_by='-timestamp')
        self.assertEqual(msg_list['total'], 6, 'Push message broken')
        self.assertEqual(msg_list['messages'][0]['id'], result.uuid, 'Push message broken')
