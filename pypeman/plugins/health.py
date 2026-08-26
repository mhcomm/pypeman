"""Plugin serving a health report of the running pypeman over HTTP.

`GET /health` (prefix configurable via `settings.HEALTH_CONFIG["url"]`)
on the shared plugins web app answers a JSON document: overall
ok/degraded status, version, process and event-loop info, and one
entry per channel (status, in-flight processing time, retry state,
store count, last message/error). `GET /health/channels/<name>`
answers the entry of a single channel.

Counters and timestamps come from the shared
:obj:`pypeman.plugins.stats.stats_collector`; retry data comes from
the channels' retry stores (retry replays don't fire message events).
"""

from __future__ import annotations

import asyncio
import os
import platform
import resource
import time
from datetime import datetime
from logging import getLogger

from aiohttp import web

import pypeman
from pypeman import channels
from pypeman.conf import settings
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import BundledWebappPluginMixin
from pypeman.plugins.stats import stats_collector

logger = getLogger(__name__)

DEFAULT_URL = "/health"


def _iso(epoch: float | None) -> str | None:
    return datetime.fromtimestamp(epoch).isoformat() if epoch is not None else None


def _ago(epoch: float | None, now: float) -> dict | None:
    if epoch is None:
        return None
    return {"at": _iso(epoch), "seconds_ago": round(now - epoch, 3)}


def _rss_bytes() -> int | None:
    """Current resident set size, or None when /proc is not available."""
    try:
        with open("/proc/self/statm") as statm:
            pages = int(statm.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


class HealthPlugin(BasePlugin, BundledWebappPluginMixin):
    """Serves a JSON health report of the running pypeman."""

    def __init__(self):
        conf = settings.HEALTH_CONFIG
        # never raise here (plugin ctors run for every CLI command):
        # the url is validated in webapp_urls, at bundle start
        self._url = str(conf.get("url") or DEFAULT_URL).rstrip("/")
        self._error_window = conf.get("degraded_error_window", 300)
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
                f"HEALTH_CONFIG['url'] must start with '/' and not be '/' (got {self._url!r})")
        return [
            web.get(self._url, self._get_health),
            web.get(self._url + "/channels/{channelname}", self._get_channel_health),
        ]

    async def _get_health(self, request: web.Request) -> web.Response:
        now = time.time()
        channel_docs = [await self._channel_doc(chan, now) for chan in channels.all_channels]
        started_at = stats_collector.started_at

        by_state: dict[str, int] = {}
        for doc in channel_docs:
            by_state[doc["status"]] = by_state.get(doc["status"], 0) + 1

        return web.json_response({
            "status": self._overall_status(now),
            "version": pypeman.__version__,
            "now": datetime.fromtimestamp(now).isoformat(),
            "process": {
                "pid": os.getpid(),
                "hostname": platform.node(),
                "python_version": platform.python_version(),
                "started_at": _iso(started_at),
                "uptime_seconds": round(now - started_at, 3) if started_at else None,
                "rss_bytes": _rss_bytes(),
                # ru_maxrss is in KiB on linux
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            },
            "event_loop": {
                "pending_tasks": len(asyncio.all_tasks()),
                "lag_seconds": round(stats_collector.loop_lag, 6),
            },
            "channels_by_state": by_state,
            "totals": stats_collector.global_totals(),
            "channels": channel_docs,
        })

    async def _get_channel_health(self, request: web.Request) -> web.Response:
        chan = channels.get_channel(request.match_info["channelname"])
        if chan is None:
            raise web.HTTPNotFound(
                reason=f"no channel named {request.match_info['channelname']!r}")
        return web.json_response(await self._channel_doc(chan, time.time()))

    def _overall_status(self, now: float) -> str:
        """'degraded' when a channel is PAUSED or errored recently, else 'ok'."""
        for chan in channels.all_channels:
            if chan.status == channels.BaseChannel.PAUSED:
                return "degraded"
            stats = stats_collector.channel_stats(chan.name)
            if (self._error_window and stats and stats.last_error_at is not None
                    and now - stats.last_error_at < self._error_window):
                return "degraded"
        return "ok"

    async def _channel_doc(self, chan, now: float) -> dict:
        stats = stats_collector.channel_stats(chan.name)
        store = chan.message_store
        return {
            "name": chan.name,
            "status": chan.status_id_to_str(chan.status),
            "processing_seconds": stats_collector.processing_seconds(chan.name),
            "messages": stats.msg_count if stats else 0,
            "errors": stats.error_count if stats else 0,
            "retry_deferred": stats.retry_deferred_count if stats else 0,
            "last_message": _ago(stats.last_msg_end_at, now) if stats else None,
            "last_error": (
                dict(_ago(stats.last_error_at, now), message=stats.last_error_text)
                if stats and stats.last_error_at is not None else None),
            "retry": await self._retry_doc(chan, now),
            "store": {
                "present": chan.has_message_store,
                # a store only has a total once the channel started it
                "total": (await store.total()
                          if chan.has_message_store and store._active else None),
            },
        }

    async def _retry_doc(self, chan, now: float) -> dict | None:
        retry_store = getattr(chan, "retry_store", None)
        if retry_store is None:
            return None
        since = retry_store.retry_mode_since
        return {
            "active": retry_store.state == retry_store.RETRY_MODE,
            "since": _iso(since),
            "seconds_in_retry": round(now - since, 3) if since is not None else None,
            "attempts": retry_store.retry_attempts,
            "pending_messages": await retry_store.total() if retry_store._active else None,
        }
