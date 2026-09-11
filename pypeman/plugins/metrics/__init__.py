"""Plugin serving per-channel message statistics over HTTP.

On the shared plugins web app (prefix configurable via
`settings.METRICS_CONFIG["url"]`):

* `GET /metrics` — Prometheus text exposition of the live counters
  and gauges (channel label = full dotted channel name: subchannels
  report the same messages as their parent, don't sum across labels);
* `GET /metrics/live` — the same live snapshot as JSON;
* `GET /metrics/channels` — JSON since-start stats for every channel;
* `GET /metrics/channels/<name>` — JSON stats for one channel;

Both accept `start_dt`/`end_dt` ISO query parameters; a `range` block
is then computed from the channel's message store metas (counts by
state, mean/min/max of the `process_time` meta written by
MsgMetaExtenderPlugin). Since-start figures come from the shared
:obj:`pypeman.plugins.metrics.stats.stats_collector` and, unlike range
figures, do not survive restarts nor cover retry replays.
"""

from pypeman.plugins.metrics.plugin import MetricsPlugin

__all__ = ("MetricsPlugin",)
