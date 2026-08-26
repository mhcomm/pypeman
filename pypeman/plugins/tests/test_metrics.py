"""Tests for the metrics plugin (JSON routes)."""

import asyncio

import pytest
from aiohttp import ClientSession

from pypeman import msgstore
from pypeman import nodes
from pypeman.channels import BaseChannel
from pypeman.conf import settings
from pypeman.plugins.base import webapp_bundle
from pypeman.plugins.metrics import MetricsPlugin
from pypeman.plugins.stats import stats_collector
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401 (fixture)


class BoomNode(nodes.BaseNode):
    """Raises on messages whose payload is 'boom'."""

    async def process(self, msg):
        if msg.payload == "boom":
            raise TstException("boom")
        return msg


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


async def _mk_handled_channel():
    """A started channel that handled 2 ok + 1 error message on
    2026-08-01 at 10:00, 11:00 and 12:00, with process_time metas
    (as ProcTimePlugin would have written) on the two ok ones."""
    chan = BaseChannel(
        name="metrics_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    chan.add(BoomNode(name="metrics_boom"))
    await chan.start()

    for hour, payload, process_time in ((10, "ok", 0.2), (11, "boom", None), (12, "ok", 0.4)):
        msg = generate_msg(timestamp=(2026, 8, 1, hour), message_content=payload)
        if payload == "boom":
            with pytest.raises(TstException):
                await chan.handle(msg)
        else:
            await chan.handle(msg)
            await chan.message_store.add_message_meta_infos(
                msg.store_id, "process_time", process_time)
    return chan


@pytest.mark.usefixtures("clear_graph")
def test_metrics_since_start_and_range(plugin_env):
    async def scenario():
        plugin = MetricsPlugin()
        await plugin.task_start()
        await _mk_handled_channel()

        async with ClientSession() as cs:
            async with cs.get(_server_url() + "/metrics/channels") as resp:
                assert resp.status == 200
                (doc,) = (await resp.json())["channels"]

            assert doc["name"] == "metrics_chan"
            assert doc["has_message_store"] is True
            since = doc["since_start"]
            assert since["messages"] == 3
            assert since["errors"] == 1
            assert since["retry_deferred"] == 0
            assert since["process_time"]["min"] <= since["process_time"]["mean"] \
                <= since["process_time"]["max"]
            assert since["store_total"] == 3
            assert since["retry_pending"] is None  # no RETRY_STORE_PATH configured
            assert "range" not in doc

            # range covering only the 11:00 and 12:00 messages
            query = "?start_dt=2026-08-01T10:30:00&end_dt=2026-08-01T12:30:00"
            async with cs.get(
                    _server_url() + "/metrics/channels/metrics_chan" + query) as resp:
                assert resp.status == 200
                rng = (await resp.json())["range"]
            assert rng["messages"] == 2
            assert rng["by_state"] == {"error": 1, "processed": 1}
            assert rng["errors"] == 1
            # only the 12:00 ok message carries a process_time meta
            assert rng["process_time"] == {"count": 1, "mean": 0.4, "min": 0.4, "max": 0.4}
            assert rng["throughput_per_second"] == round(2 / 7200, 6)

            # half-open range: no throughput
            async with cs.get(
                    _server_url() + "/metrics/channels/metrics_chan"
                    + "?end_dt=2026-08-01T10:30:00") as resp:
                rng = (await resp.json())["range"]
            assert rng["messages"] == 1
            assert rng["throughput_per_second"] is None

        await plugin.task_stop()

    asyncio.run(scenario())


@pytest.mark.usefixtures("clear_graph")
def test_metrics_error_cases(plugin_env):
    async def scenario():
        plugin = MetricsPlugin()
        await plugin.task_start()
        storeless = BaseChannel(name="metrics_storeless")
        await storeless.start()

        async with ClientSession() as cs:
            async with cs.get(_server_url() + "/metrics/channels/nope") as resp:
                assert resp.status == 404
            for bad_query in (
                "?start_dt=not-a-date",
                "?start_dt=2026-08-02T00:00:00&end_dt=2026-08-01T00:00:00",
                "?start_dt=2026-08-01T00:00:00&end_dt=2026-08-01T00:00:00",
            ):
                async with cs.get(
                        _server_url() + "/metrics/channels/metrics_storeless" + bad_query) as resp:
                    assert resp.status == 400

            # per-channel range on a storeless channel: 400...
            query = "?start_dt=2026-08-01T00:00:00&end_dt=2026-08-02T00:00:00"
            async with cs.get(
                    _server_url() + "/metrics/channels/metrics_storeless" + query) as resp:
                assert resp.status == 400
            # ...but the all-channels route just nulls its range
            async with cs.get(_server_url() + "/metrics/channels" + query) as resp:
                (doc,) = (await resp.json())["channels"]
                assert doc["range"] is None

        await plugin.task_stop()

    asyncio.run(scenario())


@pytest.mark.usefixtures("clear_graph")
def test_metrics_custom_url_and_validation(plugin_env, monkeypatch):
    async def scenario():
        monkeypatch.setitem(settings.__dict__, "METRICS_CONFIG", {"url": "/stats/"})
        plugin = MetricsPlugin()
        assert plugin._url == "/stats"
        await plugin.task_start()
        async with ClientSession() as cs:
            async with cs.get(_server_url() + "/stats/channels") as resp:
                assert resp.status == 200
        await plugin.task_stop()

        for bad_url in ("oops", "/"):
            monkeypatch.setitem(settings.__dict__, "METRICS_CONFIG", {"url": bad_url})
            bad_plugin = MetricsPlugin()  # ctor must not raise
            with pytest.raises(ValueError):
                bad_plugin.webapp_urls()

    asyncio.run(scenario())
