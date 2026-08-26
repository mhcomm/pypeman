"""Shared in-memory channel statistics collector.

Fed by the :mod:`pypeman.events` message and state events, it tracks
per-channel counters and timestamps that exist nowhere else (channels
keep no timing or error history). The health and metrics plugins both
read from the module-level :obj:`stats_collector` singleton; whichever
of them is enabled starts it once (same pattern as `webapp_bundle`).

Retry replays are injected without going through `BaseChannel.handle`,
so they never fire the message events: counters here cover first
attempts and deferrals only. Exact retry data lives on
`channel.retry_store` (`retry_attempts`, `retry_mode_since`).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from time import perf_counter

from pypeman import events
from pypeman import exceptions
from pypeman.channels import BaseChannel


class ChannelStats:
    """Since-start counters and timestamps for one channel.

    Wall-clock values are `time.time()` seconds; durations come from
    `perf_counter` differences.
    """

    def __init__(self, has_parent: bool = False):
        self.has_parent = has_parent  # subchannels are excluded from global totals
        self.msg_count = 0  # completed handle() calls, errors and deferrals included
        self.error_count = 0
        self.retry_deferred_count = 0  # ended on RetryException/PausedChanException
        self.time_count = 0  # messages with a measured duration
        self.time_sum = 0.0
        self.time_min: float | None = None
        self.time_max: float | None = None
        self.last_msg_end_at: float | None = None
        self.last_error_at: float | None = None
        self.last_error_text: str | None = None
        self.paused_since: float | None = None


class StatsCollector:
    """Event-fed statistics, shared by the health and metrics plugins.

    `start_once`/`stop_once` are idempotent so both plugins can call
    them from their `task_start`/`task_stop`; `stop_once` is safe
    without a start. Handlers run in the message path and stay O(1).
    """

    HEARTBEAT_PERIOD = 1.0  # seconds between event-loop lag measurements

    def __init__(self):
        self._reset()

    def _reset(self):
        """Test hook: forget all collected state."""
        self._started = False
        self._subscribed = False
        self._heartbeat_task: asyncio.Task | None = None
        self.started_at: float | None = None
        self.loop_lag = 0.0
        self._stats: dict[str, ChannelStats] = {}
        # (channel name, message uuid) -> (perf_counter, time.time());
        # same correlation key as ProcTimePlugin
        self._inflight: dict[tuple[str, str], tuple[float, float]] = {}

    async def start_once(self):
        if self._started:
            return
        # no await between the check and the set: atomic on the loop
        self._started = True
        self.started_at = time.time()
        events.msg_processing_start.add_handler(self._on_start)
        events.msg_processing_end.add_handler(self._on_end)
        events.channel_change_state.add_handler(self._on_state_change)
        self._subscribed = True
        self._heartbeat_task = asyncio.get_running_loop().create_task(self._heartbeat())

    async def stop_once(self):
        task, self._heartbeat_task = self._heartbeat_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._subscribed:
            self._subscribed = False
            events.msg_processing_start.remove_handler(self._on_start)
            events.msg_processing_end.remove_handler(self._on_end)
            events.channel_change_state.remove_handler(self._on_state_change)

    async def _heartbeat(self):
        """Measure how late `sleep` wakeups are: a loose event-loop lag gauge."""
        loop = asyncio.get_running_loop()
        while True:
            before = loop.time()
            await asyncio.sleep(self.HEARTBEAT_PERIOD)
            self.loop_lag = max(0.0, loop.time() - before - self.HEARTBEAT_PERIOD)

    def _stats_for(self, channel) -> ChannelStats:
        stats = self._stats.get(channel.name)
        if stats is None:
            stats = self._stats[channel.name] = ChannelStats(
                has_parent=channel.parent is not None)
        return stats

    async def _on_start(self, channel, msg):
        self._inflight[(channel.name, msg.uuid)] = (perf_counter(), time.time())

    async def _on_end(self, channel, msg, result, exc):
        entry = self._inflight.pop((channel.name, msg.uuid), None)
        stats = self._stats_for(channel)
        now = time.time()
        stats.msg_count += 1
        stats.last_msg_end_at = now
        if isinstance(exc, (exceptions.RetryException, exceptions.PausedChanException)):
            stats.retry_deferred_count += 1
        elif exc is not None:
            stats.error_count += 1
            stats.last_error_at = now
            stats.last_error_text = str(exc) or repr(exc)
        if entry is not None:  # start handler did not run (collector started mid-flight)
            duration = perf_counter() - entry[0]
            stats.time_count += 1
            stats.time_sum += duration
            stats.time_min = duration if stats.time_min is None else min(stats.time_min, duration)
            stats.time_max = duration if stats.time_max is None else max(stats.time_max, duration)

    async def _on_state_change(self, channel, old_state, new_state):
        stats = self._stats_for(channel)
        if new_state == BaseChannel.PAUSED:
            if stats.paused_since is None:
                stats.paused_since = time.time()
        elif old_state == BaseChannel.PAUSED:
            stats.paused_since = None

    def channel_stats(self, name: str) -> ChannelStats | None:
        """Stats for a channel name, or None if it never fired an event."""
        return self._stats.get(name)

    def processing_seconds(self, name: str) -> float | None:
        """Seconds the oldest in-flight message of a channel has been processing."""
        walls = [wall for (chan_name, _uuid), (_perf, wall) in self._inflight.items()
                 if chan_name == name]
        if not walls:
            return None
        return time.time() - min(walls)

    def global_totals(self) -> dict:
        """Message/error/deferral totals over top-level channels only.

        Subchannels fire their own event pair for the same message, so
        including them would double-count.
        """
        totals = {"messages": 0, "errors": 0, "retry_deferred": 0}
        for stats in self._stats.values():
            if stats.has_parent:
                continue
            totals["messages"] += stats.msg_count
            totals["errors"] += stats.error_count
            totals["retry_deferred"] += stats.retry_deferred_count
        return totals


stats_collector = StatsCollector()
