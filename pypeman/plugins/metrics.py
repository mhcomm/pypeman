"""Plugin serving per-channel message statistics over HTTP.

On the shared plugins web app (prefix configurable via
`settings.METRICS_CONFIG["url"]`):

* `GET /metrics` — Prometheus text exposition of the live counters
  and gauges (channel label = full dotted channel name: subchannels
  report the same messages as their parent, don't sum across labels);
* `GET /metrics/channels` — JSON since-start stats for every channel;
* `GET /metrics/channels/<name>` — JSON stats for one channel;

Both accept `start_dt`/`end_dt` ISO query parameters; a `range` block
is then computed from the channel's message store metas (counts by
state, mean/min/max of the `process_time` meta written by
ProcTimePlugin). Since-start figures come from the shared
:obj:`pypeman.plugins.stats.stats_collector` and, unlike range
figures, do not survive restarts nor cover retry replays.
"""

from __future__ import annotations

import asyncio
import platform
from datetime import datetime
from logging import getLogger

from aiohttp import web
from dateutil import parser as dateutilparser

import pypeman
from pypeman import channels
from pypeman.conf import settings
from pypeman.message import Message
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import BundledWebappPluginMixin
from pypeman.plugins.proctime import ProcTimePlugin
from pypeman.plugins.stats import rss_bytes
from pypeman.plugins.stats import stats_collector

logger = getLogger(__name__)

DEFAULT_URL = "/metrics"
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _parse_query_dt(request: web.Request, name: str) -> datetime | None:
    """Parse an ISO datetime query parameter, naive local (like store timestamps)."""
    raw = request.rel_url.query.get(name)
    if raw is None:
        return None
    try:
        parsed = dateutilparser.isoparse(raw)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"invalid ISO datetime for {name!r}: {raw!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _sample(name: str, labels: dict | None, value) -> str:
    """One Prometheus sample line."""
    rendered = repr(float(value)) if isinstance(value, float) else str(value)
    if labels:
        inner = ",".join(f'{key}="{_escape_label(str(val))}"' for key, val in labels.items())
        return f"{name}{{{inner}}} {rendered}"
    return f"{name} {rendered}"


def _aggregate_metas(metas: list[dict]) -> dict:
    """Aggregate store metas of a time span into a stats dict."""
    by_state: dict[str, int] = {}
    times = []
    for meta in metas:
        state = meta.get("state")
        by_state[state] = by_state.get(state, 0) + 1
        try:
            times.append(float(meta[ProcTimePlugin.META_NAME]))
        except (KeyError, TypeError, ValueError):
            pass
    return {
        "messages": len(metas),
        "by_state": by_state,
        "errors": by_state.get(Message.ERROR, 0) + by_state.get(Message.REJECTED, 0),
        "process_time": {
            "count": len(times),
            "mean": round(sum(times) / len(times), 6),
            "min": min(times),
            "max": max(times),
        } if times else None,
    }


class MetricsPlugin(BasePlugin, BundledWebappPluginMixin):
    """Serves per-channel message statistics as JSON."""

    def __init__(self):
        conf = settings.METRICS_CONFIG
        # never raise here (plugin ctors run for every CLI command):
        # the url is validated in webapp_urls, at bundle start
        self._url = str(conf.get("url") or DEFAULT_URL).rstrip("/")
        super().__init__()  # registers into the webapp bundle

    async def task_start(self):
        await super().task_start()  # webapp bundle
        await stats_collector.start_once()

    async def task_stop(self):
        await stats_collector.stop_once()
        await super().task_stop()

    def webapp_prefix(self) -> str:
        # root-mounted: a non-empty bundle prefix would 404 the bare
        # url (aiohttp sub-apps only answer with a trailing slash)
        return ""

    def webapp_urls(self) -> list[web.RouteDef]:
        if not self._url.startswith("/") or "/" == self._url:
            raise ValueError(
                f"METRICS_CONFIG['url'] must start with '/' and not be '/' (got {self._url!r})")
        return [
            web.get(self._url, self._get_prometheus),
            web.get(self._url + "/channels", self._get_channels),
            web.get(self._url + "/channels/{channelname}", self._get_channel),
        ]

    async def _get_channels(self, request: web.Request) -> web.Response:
        span = self._request_span(request)
        docs = []
        for chan in channels.all_channels:
            doc = await self._channel_doc(chan)
            if span is not None:
                # storeless channels just get a null range here
                doc["range"] = (await self._range_doc(chan, span)
                                if self._has_active_store(chan) else None)
            docs.append(doc)
        return web.json_response({"channels": docs})

    async def _get_channel(self, request: web.Request) -> web.Response:
        chan = channels.get_channel(request.match_info["channelname"])
        if chan is None:
            raise web.HTTPNotFound(
                reason=f"no channel named {request.match_info['channelname']!r}")
        doc = await self._channel_doc(chan)
        span = self._request_span(request)
        if span is not None:
            if not self._has_active_store(chan):
                raise web.HTTPBadRequest(
                    reason=f"channel {chan.name!r} has no (started) message store")
            doc["range"] = await self._range_doc(chan, span)
        return web.json_response(doc)

    @staticmethod
    def _request_span(request: web.Request) -> tuple[datetime | None, datetime | None] | None:
        """The requested time range, or None when no bound was given."""
        start_dt = _parse_query_dt(request, "start_dt")
        end_dt = _parse_query_dt(request, "end_dt")
        if start_dt is None and end_dt is None:
            return None
        if start_dt is not None and end_dt is not None and not start_dt < end_dt:
            raise web.HTTPBadRequest(reason="start_dt must be strictly before end_dt")
        return start_dt, end_dt

    @staticmethod
    def _has_active_store(chan) -> bool:
        # message stores only initialize in channel.start()
        return chan.has_message_store and chan.message_store._active

    async def _channel_doc(self, chan) -> dict:
        stats = stats_collector.channel_stats(chan.name)
        retry_store = getattr(chan, "retry_store", None)
        return {
            "name": chan.name,
            "status": chan.status_id_to_str(chan.status),
            "has_message_store": chan.has_message_store,
            "since_start": {
                "messages": stats.msg_count if stats else 0,
                "errors": stats.error_count if stats else 0,
                "retry_deferred": stats.retry_deferred_count if stats else 0,
                "process_time": {
                    "mean": round(stats.time_sum / stats.time_count, 6),
                    "min": stats.time_min,
                    "max": stats.time_max,
                } if stats and stats.time_count else None,
                "store_total": (await chan.message_store.total()
                                if self._has_active_store(chan) else None),
                "retry_pending": (await retry_store.total()
                                  if retry_store is not None and retry_store._active else None),
            },
        }

    async def _range_doc(self, chan, span: tuple) -> dict:
        start_dt, end_dt = span
        metas = await chan.message_store.get_message_metas(start_dt, end_dt)
        doc = {
            "start_dt": start_dt.isoformat() if start_dt else None,
            "end_dt": end_dt.isoformat() if end_dt else None,
        }
        doc.update(_aggregate_metas(metas))
        if start_dt is not None and end_dt is not None:
            span_seconds = (end_dt - start_dt).total_seconds()
            doc["throughput_per_second"] = round(doc["messages"] / span_seconds, 6)
        else:
            doc["throughput_per_second"] = None
        return doc

    async def _get_prometheus(self, request: web.Request) -> web.Response:
        """Prometheus text exposition; live counters and O(1) store
        totals only, no store scan on scrape."""
        lines = []

        def family(name, ftype, help_text):
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {ftype}")

        family("pypeman_info", "gauge", "Version information of the running pypeman.")
        lines.append(_sample("pypeman_info", {
            "version": pypeman.__version__, "python": platform.python_version()}, 1))

        if stats_collector.started_at is not None:
            family("pypeman_process_start_time_seconds", "gauge",
                   "Unix time the process started at.")
            lines.append(_sample(
                "pypeman_process_start_time_seconds", None, stats_collector.started_at))

        rss = rss_bytes()
        if rss is not None:
            family("pypeman_process_resident_memory_bytes", "gauge",
                   "Resident memory size in bytes.")
            lines.append(_sample("pypeman_process_resident_memory_bytes", None, rss))

        family("pypeman_event_loop_lag_seconds", "gauge",
               "Last measured event-loop wakeup lag.")
        lines.append(_sample("pypeman_event_loop_lag_seconds", None, stats_collector.loop_lag))
        family("pypeman_pending_tasks", "gauge", "Pending asyncio tasks.")
        lines.append(_sample("pypeman_pending_tasks", None, len(asyncio.all_tasks())))

        chans = list(channels.all_channels)

        family("pypeman_channel_state", "gauge", "Channel state as a one-hot gauge.")
        for chan in chans:
            current = chan.status_id_to_str(chan.status)
            for state in channels.BaseChannel.STATE_NAMES:
                lines.append(_sample("pypeman_channel_state",
                                     {"channel": chan.name, "state": state},
                                     int(state == current)))

        for name, help_text, attr in (
            ("pypeman_channel_messages_total",
             "Messages handled since start (first attempts and deferrals, not retries).",
             "msg_count"),
            ("pypeman_channel_errors_total", "Messages ended in error since start.",
             "error_count"),
            ("pypeman_channel_retry_deferred_total", "Messages deferred to retry since start.",
             "retry_deferred_count"),
        ):
            family(name, "counter", help_text)
            for chan in chans:
                stats = stats_collector.channel_stats(chan.name)
                lines.append(_sample(name, {"channel": chan.name},
                                     getattr(stats, attr) if stats else 0))

        family("pypeman_channel_processing_seconds", "summary", "Message processing time.")
        for chan in chans:
            stats = stats_collector.channel_stats(chan.name)
            labels = {"channel": chan.name}
            lines.append(_sample("pypeman_channel_processing_seconds_sum", labels,
                                 stats.time_sum if stats else 0.0))
            lines.append(_sample("pypeman_channel_processing_seconds_count", labels,
                                 stats.time_count if stats else 0))

        for suffix, attr in (("min", "time_min"), ("max", "time_max")):
            samples = []
            for chan in chans:
                stats = stats_collector.channel_stats(chan.name)
                if stats and stats.time_count:
                    samples.append(_sample(f"pypeman_channel_processing_seconds_{suffix}",
                                           {"channel": chan.name}, getattr(stats, attr)))
            if samples:
                family(f"pypeman_channel_processing_seconds_{suffix}", "gauge",
                       f"{suffix.capitalize()} message processing time since start.")
                lines.extend(samples)

        for name, help_text, getter in (
            ("pypeman_channel_store_messages", "Messages in the channel's message store.",
             lambda chan: chan.message_store if self._has_active_store(chan) else None),
            ("pypeman_channel_retry_pending", "Messages awaiting retry.",
             lambda chan: (chan.retry_store
                           if getattr(chan, "retry_store", None) is not None
                           and chan.retry_store._active else None)),
        ):
            samples = []
            for chan in chans:
                store = getter(chan)
                if store is not None:
                    samples.append(_sample(name, {"channel": chan.name}, await store.total()))
            if samples:
                family(name, "gauge", help_text)
                lines.extend(samples)

        body = "\n".join(lines) + "\n"
        return web.Response(body=body.encode("utf-8"),
                            headers={"Content-Type": PROMETHEUS_CONTENT_TYPE})
