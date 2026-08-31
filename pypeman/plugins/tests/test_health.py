"""Tests for the health plugin."""

import asyncio

import pytest
from aiohttp import ClientSession

from pypeman import msgstore
from pypeman.channels import BaseChannel
from pypeman.conf import settings
from pypeman.exceptions import Dropped
from pypeman.nodes import Drop
from pypeman.exceptions import PausedChanException
from pypeman.nodes import BaseNode
from pypeman.nodes import Sleep
from pypeman.plugins.base import webapp_bundle
from pypeman.plugins.health import HealthPlugin
from pypeman.plugins.stats import stats_collector
from pypeman.tests.common import ExceptNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401 (fixture)


@pytest.fixture
def plugin_env(monkeypatch):
    """Fresh bundle + collector, webapp on a free port."""
    monkeypatch.setitem(
        settings.__dict__, "WEBAPP_PLUGINS_CONFIG", {"host": "127.0.0.1", "port": 0}
    )
    webapp_bundle._reset()
    stats_collector._reset()
    yield
    webapp_bundle._reset()
    stats_collector._reset()


def _server_url():
    host, port = webapp_bundle._runner.addresses[0][:2]
    return f"http://{host}:{port}"


@pytest.mark.usefixtures("clear_graph")
def test_health_document(plugin_env):
    async def scenario():
        chan = BaseChannel(
            name="health_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
        chan.add(Sleep(name="health_sleep", duration=0))

        plugin = HealthPlugin()
        await plugin.task_start()
        await chan.start()
        await chan.handle(generate_msg())

        async with ClientSession() as cs:
            # the bare url must answer (no trailing slash needed)
            async with cs.get(_server_url() + "/health") as resp:
                assert resp.status == 200
                doc = await resp.json()

        assert doc["status"] == "ok"
        assert doc["version"]
        assert doc["process"]["pid"] > 0
        assert doc["process"]["uptime_seconds"] >= 0
        assert doc["event_loop"]["pending_tasks"] >= 1
        assert doc["channels_by_state"] == {"WAITING": 1}
        assert doc["totals"] == {
            "messages": 1, "errors": 0, "dropped": 0, "retry_deferred": 0}

        (chan_doc,) = doc["channels"]
        assert chan_doc["name"] == "health_chan"
        assert chan_doc["status"] == "WAITING"
        assert chan_doc["processing_seconds"] is None
        assert chan_doc["messages"] == 1
        assert chan_doc["errors"] == 0
        assert chan_doc["dropped"] == 0
        assert chan_doc["last_message"]["seconds_ago"] >= 0
        assert chan_doc["last_error"] is None
        assert chan_doc["retry"] is None  # no RETRY_STORE_PATH configured
        assert chan_doc["store"] == {"present": True, "total": 1}

        await plugin.task_stop()

    asyncio.run(scenario())


@pytest.mark.usefixtures("clear_graph")
def test_health_degraded_and_channel_route(plugin_env, monkeypatch):
    async def scenario():
        chan = BaseChannel(name="health_bad_chan")
        chan.add(ExceptNode(name="health_bad_node"))

        plugin = HealthPlugin()
        await plugin.task_start()
        await chan.start()
        with pytest.raises(TstException):
            await chan.handle(generate_msg())

        async with ClientSession() as cs:
            # recent error within the default window: degraded
            async with cs.get(_server_url() + "/health") as resp:
                doc = await resp.json()
            assert doc["status"] == "degraded"
            assert doc["channels"][0]["last_error"]["message"]

            # per-channel route, and 404 on unknown names
            async with cs.get(_server_url() + "/health/channels/health_bad_chan") as resp:
                assert resp.status == 200
                chan_doc = await resp.json()
            assert chan_doc["errors"] == 1
            assert chan_doc["store"] == {"present": False, "total": None}
            async with cs.get(_server_url() + "/health/channels/nope") as resp:
                assert resp.status == 404

            # window disabled: errors no longer degrade, but PAUSED does
            monkeypatch.setattr(plugin, "_error_window", 0)
            async with cs.get(_server_url() + "/health") as resp:
                assert (await resp.json())["status"] == "ok"
            chan.status = BaseChannel.PAUSED
            async with cs.get(_server_url() + "/health") as resp:
                assert (await resp.json())["status"] == "degraded"
            chan.status = BaseChannel.WAITING

        await plugin.task_stop()

    asyncio.run(scenario())


@pytest.mark.usefixtures("clear_graph")
def test_dropped_does_not_degrade(plugin_env):
    """`chan.fork().add(Drop())` is deliberate flow control, not an error."""
    async def scenario():
        chan = BaseChannel(name="health_drop_chan", wait_subchans=True)
        forked = chan.fork(name="health_drop_fork")
        forked.add(Drop(name="health_drop_node"))

        plugin = HealthPlugin()
        await plugin.task_start()
        await chan.start()
        await forked.start()
        with pytest.raises(Dropped):  # propagated by the gathered subchan task
            await chan.handle(generate_msg())

        async with ClientSession() as cs:
            async with cs.get(_server_url() + "/health") as resp:
                doc = await resp.json()

        assert doc["status"] == "ok"
        assert doc["totals"] == {
            "messages": 1, "errors": 0, "dropped": 1, "retry_deferred": 0}
        by_name = {chan_doc["name"]: chan_doc for chan_doc in doc["channels"]}
        for name in ("health_drop_chan", "health_drop_chan.health_drop_fork"):
            assert by_name[name]["dropped"] == 1
            assert by_name[name]["errors"] == 0
            assert by_name[name]["last_error"] is None

        await plugin.task_stop()

    asyncio.run(scenario())


@pytest.mark.usefixtures("clear_graph")
def test_health_custom_url_and_validation(plugin_env, monkeypatch):
    async def scenario():
        monkeypatch.setitem(settings.__dict__, "HEALTH_CONFIG", {"url": "/sante/"})
        plugin = HealthPlugin()
        assert plugin._url == "/sante"
        await plugin.task_start()
        async with ClientSession() as cs:
            async with cs.get(_server_url() + "/sante") as resp:
                assert resp.status == 200
        await plugin.task_stop()

        for bad_url in ("oops", "/"):
            monkeypatch.setitem(settings.__dict__, "HEALTH_CONFIG", {"url": bad_url})
            bad_plugin = HealthPlugin()  # ctor must not raise
            with pytest.raises(ValueError):
                bad_plugin.webapp_urls()

    asyncio.run(scenario())


class RetryingNode(BaseNode):
    """Node deferring its message until `raise_exc` is cleared."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raise_exc = True

    def process(self, msg):
        if self.raise_exc:
            raise TstException()
        return msg


@pytest.mark.usefixtures("clear_graph")
def test_health_retry_document(plugin_env, monkeypatch, tmp_path):
    monkeypatch.setitem(settings.__dict__, "RETRY_STORE_PATH", tmp_path)

    async def scenario():
        chan = BaseChannel(
            name="health_retry_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
        node = RetryingNode(name="health_retry_node", auto_retry_exceptions=[TstException])
        chan.add(node)

        plugin = HealthPlugin()
        await plugin.task_start()
        chan._reset_test()  # no retry task: the replays are driven by hand below
        await chan.start()
        with pytest.raises(PausedChanException):
            await chan.handle(generate_msg())

        async with ClientSession() as cs:
            async def retry_doc():
                url = _server_url() + "/health/channels/health_retry_chan"
                async with cs.get(url) as resp:
                    assert resp.status == 200
                    return (await resp.json())["retry"]

            # just entered retry mode: dated, and no failed replay yet
            doc = await retry_doc()
            assert doc["active"] is True
            assert doc["since"] is not None
            assert doc["seconds_in_retry"] >= 0
            assert doc["attempts"] == 0
            assert doc["pending_messages"] == 1

            # one replay, the node still fails
            await chan.retry_store.retry()
            doc = await retry_doc()
            assert doc["active"] is True
            assert doc["attempts"] == 1
            assert doc["pending_messages"] == 1

            # the node recovers: the successful replay is not counted as an attempt,
            # and leaving retry mode clears the counter
            node.raise_exc = False
            await chan.retry_store.retry()
            doc = await retry_doc()
            assert doc["active"] is False
            assert doc["since"] is None
            assert doc["seconds_in_retry"] is None
            assert doc["attempts"] == 0
            assert doc["pending_messages"] == 0

        await plugin.task_stop()
        await chan.stop()

    asyncio.run(scenario())
