"""Tests for the shared channel statistics collector."""

import asyncio

import pytest

from pypeman import events
from pypeman import exceptions
from pypeman.channels import BaseChannel
from pypeman.nodes import Sleep
from pypeman.plugins.stats import stats_collector
from pypeman.tests.common import ExceptNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401 (fixture)


@pytest.fixture
async def collector():
    """A started collector, stopped (hence unsubscribed) afterwards."""
    stats_collector._reset()
    await stats_collector.start_once()
    yield stats_collector
    await stats_collector.stop_once()
    stats_collector._reset()


@pytest.mark.usefixtures("clear_graph")
async def test_counts_and_timing(collector):
    chan = BaseChannel(name="stats_chan")
    chan.add(Sleep(name="stats_sleep", duration=0.05))
    await chan.start()

    await chan.handle(generate_msg())
    await chan.handle(generate_msg())

    stats = collector.channel_stats("stats_chan")
    assert stats.msg_count == 2
    assert stats.error_count == 0
    assert stats.retry_deferred_count == 0
    assert stats.time_count == 2
    assert stats.time_min >= 0.05
    assert stats.time_max >= stats.time_min
    assert stats.time_sum >= stats.time_min + stats.time_max - 1e-6
    assert stats.last_msg_end_at is not None
    assert stats.last_error_at is None
    assert not collector._inflight  # no leak
    assert collector.processing_seconds("stats_chan") is None


@pytest.mark.usefixtures("clear_graph")
async def test_error_recording(collector):
    chan = BaseChannel(name="stats_exc_chan")
    chan.add(ExceptNode(name="stats_exc_node"))
    await chan.start()

    with pytest.raises(TstException):
        await chan.handle(generate_msg())

    stats = collector.channel_stats("stats_exc_chan")
    assert stats.msg_count == 1
    assert stats.error_count == 1
    assert stats.last_error_at is not None
    assert stats.last_error_text
    assert not collector._inflight


@pytest.mark.usefixtures("clear_graph")
async def test_retry_deferred_is_not_an_error(collector):
    chan = BaseChannel(name="stats_retry_chan")
    await chan.start()

    msg = generate_msg()
    await events.msg_processing_start.fire(channel=chan, msg=msg)
    await events.msg_processing_end.fire(
        channel=chan, msg=msg, result=None, exc=exceptions.RetryException())

    stats = collector.channel_stats("stats_retry_chan")
    assert stats.msg_count == 1
    assert stats.retry_deferred_count == 1
    assert stats.error_count == 0
    assert stats.last_error_at is None


@pytest.mark.usefixtures("clear_graph")
async def test_paused_since(collector):
    chan = BaseChannel(name="stats_paused_chan")
    await chan.start()

    chan.status = BaseChannel.PAUSED
    await asyncio.sleep(0)  # state event is fired via create_task
    assert collector.channel_stats("stats_paused_chan").paused_since is not None

    chan.status = BaseChannel.WAITING
    await asyncio.sleep(0)
    assert collector.channel_stats("stats_paused_chan").paused_since is None


@pytest.mark.usefixtures("clear_graph")
async def test_processing_seconds_while_inflight(collector):
    chan = BaseChannel(name="stats_inflight_chan")
    chan.add(Sleep(name="stats_inflight_sleep", duration=0.05))
    await chan.start()

    task = asyncio.get_running_loop().create_task(chan.handle(generate_msg()))
    await asyncio.sleep(0.02)
    elapsed = collector.processing_seconds("stats_inflight_chan")
    assert elapsed is not None and elapsed >= 0.01
    await task


@pytest.mark.usefixtures("clear_graph")
async def test_global_totals_skip_subchannels(collector):
    chan = BaseChannel(name="stats_fork_chan", wait_subchans=True)
    forked = chan.fork(name="stats_forked")
    forked.add(Sleep(name="stats_forked_sleep", duration=0))
    await chan.start()
    await forked.start()

    await chan.handle(generate_msg())

    # both channels have their own per-channel entry...
    assert collector.channel_stats("stats_fork_chan").msg_count == 1
    assert collector.channel_stats("stats_fork_chan.stats_forked").msg_count == 1
    # ...but global totals only count the top-level channel
    assert collector.global_totals() == {"messages": 1, "errors": 0, "retry_deferred": 0}


async def test_stop_once_unsubscribes_and_cancels_heartbeat():
    stats_collector._reset()
    await stats_collector.start_once()
    heartbeat = stats_collector._heartbeat_task
    assert stats_collector._on_start in events.msg_processing_start.handlers
    assert stats_collector._on_end in events.msg_processing_end.handlers
    assert stats_collector._on_state_change in events.channel_change_state.handlers
    assert heartbeat is not None and not heartbeat.done()

    await stats_collector.stop_once()
    assert stats_collector._on_start not in events.msg_processing_start.handlers
    assert stats_collector._on_end not in events.msg_processing_end.handlers
    assert stats_collector._on_state_change not in events.channel_change_state.handlers
    assert heartbeat.cancelled()

    await stats_collector.stop_once()  # idempotent
    stats_collector._reset()


async def test_stop_once_without_start_is_a_noop():
    stats_collector._reset()
    await stats_collector.stop_once()
