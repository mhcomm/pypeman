import asyncio
import json

from aiohttp import web

import pytest

from pypeman import msgstore
from pypeman import channels
from pypeman import nodes
from pypeman.channels import BaseChannel
from pypeman.plugin_mgr import manager
from pypeman.plugins.remoteadmin.plugin import RemoteAdminPlugin
from pypeman.plugins.remoteadmin.urls import init_urls
from pypeman.remoteadmin import RemoteAdminClient
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstNode


@pytest.fixture
def jsonrpcremoteclient(event_loop):
    client = RemoteAdminClient(loop=event_loop, url="ws://localhost:%d" % 8091)
    client.init()
    asyncio.set_event_loop(None)
    return client


@pytest.fixture
def webremoteclient(event_loop, aiohttp_client):
    app = web.Application()
    init_urls(app)
    return event_loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(autouse=True)
def remoteserver(event_loop):
    manager.plugin_classes = [RemoteAdminPlugin]
    manager.loop = event_loop
    manager.init_plugins()
    manager.ready_plugins()
    yield manager.start_plugins()
    manager.stop_plugins()


class RemoteAdminBaseMixin:

    @classmethod
    def setup_class(self):
        store_factory = msgstore.MemoryMessageStoreFactory()
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)
        self.chan_name = "test_remote050"
        self.chan = BaseChannel(name=self.chan_name, loop=self.loop, message_store_factory=store_factory)

        n = TstNode()
        n2 = TstNode(name="sub")
        n3 = TstNode(name="sub1")
        n4 = TstNode(name="sub2")
        n5 = TstNode(name="subsub")

        self.chan.add(n)

        sub = self.chan.fork(name="subchannel")
        sub2 = sub.fork(name="subchannel2", message_store_factory=store_factory)
        sub.append(n2, n3, n4)
        sub2.append(n5)

        for chan in channels.all_channels:
            asyncio.run(chan.start())

        self.msg = generate_msg(with_context=True)
        self.msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35), message_content="message content2")
        self.msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35), message_content="message_content3")
        self.msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content="message content4")

        asyncio.run(self.chan.message_store.store(self.msg))
        asyncio.run(self.chan.message_store.store(self.msg2))
        asyncio.run(self.chan.message_store.store(self.msg3))
        asyncio.run(self.chan.message_store.store(self.msg4))

    @classmethod
    def teardown_class(self):

        for chan in channels.all_channels:
            asyncio.run(chan.stop())
        channels.reset_pypeman_channels()
        nodes.reset_pypeman_nodes()
        self.loop.stop()
        self.loop.close()

    def setup_method(self, method):
        """
        Re-start the channel if the stop/start test fails
        """
        if self.chan.status != BaseChannel.WAITING:
            asyncio.run(self.chan.start())


class TestRemoteAdminPlugin(RemoteAdminBaseMixin):

    async def test_list_channels(self, webremoteclient):

        # Channel remote listing working
        resp = await webremoteclient.get("/channels")

        assert resp.status == 200
        json_resp = json.loads(await resp.text())
        assert len(json_resp) == 2
        assert json_resp[0]["name"] == "test_remote050"
        assert json_resp[0]["short_name"] == "test_remote050"
        assert json_resp[0]['subchannels'][0]['name'] == 'test_remote050.subchannel'
        assert json_resp[1]["name"] == "test_remote050.subchannel.subchannel2"
        assert json_resp[1]["short_name"] == "subchannel2"

    async def test_stop_n_start_channel(self, webremoteclient):
        # Channel stop working
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/stop")
        assert resp.status == 200

        assert self.chan.status == BaseChannel.STOPPED

        # Start channel
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/start")
        assert resp.status == 200

        assert self.chan.status == BaseChannel.WAITING

    async def test_search_messages(self, webremoteclient):
        params = {
            "start": 2,
            "count": 5,
            "order_by": "-timestamp"
        }
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/messages", params=params)

        assert resp.status == 200
        json_resp = json.loads(await resp.text())
        assert json_resp['total'] == 4
        assert len(json_resp['messages']) == 2
        assert json_resp['messages'][0]['id'] == self.msg3.uuid

    async def test_search_messages_by_date(self, webremoteclient):
        params = {
            "start_dt": "1982-11-27",
            "end_dt": "1982-11-28T13:00:00",
        }
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/messages", params=params)
        assert resp.status == 200
        json_resp = json.loads(await resp.text())
        assert json_resp['total'] == 4
        assert len(json_resp['messages']) == 2
        assert json_resp['messages'][0]['id'] == self.msg3.uuid
        assert json_resp['messages'][1]['id'] == self.msg2.uuid

    async def test_search_messages_with_text_flt(self, webremoteclient):
        params = {
            "text": "sage_c",
        }
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/messages", params=params)
        assert resp.status == 200
        json_resp = json.loads(await resp.text())
        assert len(json_resp['messages']) == 1
        assert json_resp['messages'][0]['id'] == self.msg3.uuid

    async def test_search_messages_with_rtext_flt(self, webremoteclient):
        params = {
            "rtext": r"\w+_\w+",
        }
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/messages", params=params)
        assert resp.status == 200
        json_resp = json.loads(await resp.text())
        assert len(json_resp['messages']) == 1
        assert json_resp['messages'][0]['id'] == self.msg3.uuid

    async def test_replay_message(self, webremoteclient):
        assert await self.chan.message_store.total() == 4
        resp = await webremoteclient.get(f"/channels/{self.chan_name}/messages/{self.msg3.uuid}/replay")
        assert resp.status == 200

        json_resp = json.loads(await resp.text())
        assert await self.chan.message_store.total() == 5

        # Clean
        await self.chan.message_store.delete(json_resp["uuid"])

    async def test_push_message(self, webremoteclient):
        pass  # actually not implemented


# TODO: Next tests (backport url) not working because the jsonrpcremoteclient call loop.run_until_complete but
# the loop is already running in the main thread (channels + remote server are running)
# and asyncio can't have 2 loops in same thread.
# Possible solutions:
#   - refactor RemoteAdminClient to have async list/start/stop/replay funcs that can be called
#       in a running event_loop. This refactoring could necessit to have a reusable websocket client.
#       To meditate
#   - ?
#
# Uncomment next class when the problem is resolved

# class TestRemoteAdminPluginBackportUrl(RemoteAdminBaseMixin):

#     async def test_list_channels(self, jsonrpcremoteclient):
#         # Channel remote listing working
#         chans = jsonrpcremoteclient.channels()

#         assert len(chans) == 1
#         assert chans[0]["name"] == "test_remote050"
#         assert chans[0]['subchannels'][0]['name'] == 'test_remote050.subchannel'

#     async def test_stop_n_start_channel(self, jsonrpcremoteclient):
#         # Channel stop working
#         jsonrpcremoteclient.stop('test_remote050')

#         assert self.chan.status == BaseChannel.STOPPED

#         # Start channel
#         jsonrpcremoteclient.start('test_remote050')

#         assert self.chan.status == BaseChannel.WAITING

#     async def test_search_messages(self, jsonrpcremoteclient):
#         resp = jsonrpcremoteclient.list_msgs(
#             channel='test_remote050', start=2, count=5, order_by='-timestamp')

#         assert resp['total'] == 4
#         assert len(resp['messages']) == 2
#         assert resp['messages'][0]['id'] == self.msg3.uuid

#     async def test_search_messages_by_date(self, jsonrpcremoteclient):
#         resp = jsonrpcremoteclient.list_msgs(
#             channel='test_remote050', start_dt="1982-11-27", end_dt="1982-11-28T13:00:00")
#         assert resp['total'] == 4
#         assert len(resp['messages']) == 2
#         assert resp['messages'][0]['id'] == self.msg2.uuid
#         assert resp['messages'][1]['id'] == self.msg3.uuid

#     async def test_search_messages_with_text_flt(self, jsonrpcremoteclient):
#         resp = jsonrpcremoteclient.list_msgs(
#             channel='test_remote050', text="sage_c")
#         assert len(resp['messages']) == 1
#         assert resp['messages'][0]['id'] == self.msg3.uuid

#     async def test_search_messages_with_rtext_flt(self, jsonrpcremoteclient):
#         resp = jsonrpcremoteclient.list_msgs(
#             channel='test_remote050', rtext=r"\w+_\w+")
#         assert len(resp['messages']) == 1
#         assert resp['messages'][0]['id'] == self.msg3.uuid

#     async def test_replay_message(self, jsonrpcremoteclient):
#         resp = jsonrpcremoteclient.replay_msg('test_remote050', [self.msg3.uuid])
#         assert len(resp) == 1
#         assert await self.chan.message_store.total() == 5

#         # Clean
#         await self.chan.message_store.delete(resp[0]["uuid"])

#     async def test_push_message(self, jsonrpcremoteclient):
#         pass  # actually not implemented
