import asyncio

import pytest
import pytest_asyncio.plugin  # noqa F401

from pypeman import msgstore
from pypeman import channels
from pypeman.channels import BaseChannel
from pypeman.remoteadmin import RemoteAdminClient
from pypeman.remoteadmin import RemoteAdminServer
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstNode


class RemoteAdminTests(TestCase):

    @pytest.fixture(autouse=True)
    def initfixture(self, unused_tcp_port):
        self.tcp_port = unused_tcp_port

    def clean_loop(self):
        # Useful to execute future callbacks
        pending = asyncio.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

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

        n = TstNode()
        n2 = TstNode(name="sub")
        n3 = TstNode(name="sub1")
        n4 = TstNode(name="sub2")

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35), message_content="message content2")
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35), message_content="message_content3")
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content="message content4")

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
        msg_list = client.list_msgs(channel='test_remote050', start=2, count=5, order_by='-timestamp')

        print(msg_list)

        self.assertEqual(msg_list['total'], 4, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], idref_msg3, 'List channel messages broken')

        # Search message with date filter
        msg_list = client.list_msgs(
            channel='test_remote050', start_dt="1982-11-27", end_dt="1982-11-28T13:00:00")

        print(msg_list)

        self.assertEqual(len(msg_list['messages']), 2, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][1]['id'], idref_msg3, 'List channel messages broken')

        # Search message with text filter
        msg_list = client.list_msgs(
            channel='test_remote050', text="sage_c")

        print(msg_list)

        self.assertEqual(len(msg_list['messages']), 1, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], idref_msg3, 'List channel messages broken')

        # Search message with regex filter
        msg_list = client.list_msgs(
            channel='test_remote050', rtext="\w+_\w+")  # noqa: W605

        print(msg_list)

        self.assertEqual(len(msg_list['messages']), 1, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], idref_msg3, 'List channel messages broken')

        # Replay message
        result = client.replay_msg('test_remote050', idref_msg3)

        msg_list = client.list_msgs(channel='test_remote050', start=0, count=5, order_by='-timestamp')
        self.assertEqual(msg_list['total'], 5, 'List channel messages broken')
        self.assertEqual(msg_list['messages'][0]['id'], result.uuid, 'Replay messages broken')

        # Push message
        result = client.push_msg(channel='test_remote050', text="Yaaay")

        msg_list = client.list_msgs(channel='test_remote050', start=0, count=5, order_by='-timestamp')
        self.assertEqual(msg_list['total'], 6, 'Push message broken')
        self.assertEqual(msg_list['messages'][0]['id'], result.uuid, 'Push message broken')

        # View message
        msg_infos = client.view_msg(channel='test_remote050', msg_id=idref_msg3)
        print(msg_infos)

        self.assertEqual(msg_infos.payload, msg3.payload, 'View messages broken')

        # Preview message
        msg_infos = client.preview_msg(channel='test_remote050', msg_id=idref_msg3)
        print(msg_infos)

        self.assertEqual(msg_infos.payload, msg3.payload[:1000], 'Preview messages broken')
