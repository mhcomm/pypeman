"""Plugin serving per-channel message statistics over HTTP.

On the shared plugins web app (prefix configurable via
`settings.METRICS_CONFIG["url"]`):

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

from datetime import datetime
from logging import getLogger

from aiohttp import web
from dateutil import parser as dateutilparser

from pypeman import channels
from pypeman.conf import settings
from pypeman.message import Message
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import BundledWebappPluginMixin
from pypeman.plugins.proctime import ProcTimePlugin
from pypeman.plugins.stats import stats_collector

logger = getLogger(__name__)

DEFAULT_URL = "/metrics"


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
