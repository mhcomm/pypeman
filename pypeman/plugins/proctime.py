"""Plugin tagging the messages with their processing time.

Enabled by default; deactivate it by adding
`"pypeman.plugins.proctime.ProcTimePlugin"` to `settings.DISABLED_PLUGINS`.
"""

from __future__ import annotations

from time import perf_counter

from pypeman import events
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import TaskPluginMixin


class ProcTimePlugin(BasePlugin, TaskPluginMixin):
    """Tag every message with the time its channel took to process it.

    The duration in seconds is written to `msg.meta["process_time"]`,
    both on the message the channel took in and on the one it returns,
    and is added to the message store entry of the former (the store
    got its copy of the message before the timing was known).

    A message forked or routed to a subchannel is timed once per
    channel it goes through, the innermost channel being the one whose
    timing ends up in the returned message.

    This plugin doubles as a reference for :mod:`pypeman.events` based
    plugins: subscribe in `task_start`, unsubscribe in `task_stop`.
    """

    META_NAME = "process_time"

    def __init__(self):
        # (channel name, message uuid) -> entry time; a message keeps
        # its uuid across channels, a channel handles it only once
        self._entry_times: dict[tuple[str, str], float] = {}

    async def task_start(self):
        events.msg_processing_start.add_handler(self._on_start)
        events.msg_processing_end.add_handler(self._on_end)

    async def task_stop(self):
        events.msg_processing_start.remove_handler(self._on_start)
        events.msg_processing_end.remove_handler(self._on_end)

    async def _on_start(self, channel, msg):
        self._entry_times[(channel.name, msg.uuid)] = perf_counter()

    async def _on_end(self, channel, msg, result, exc):
        entry_time = self._entry_times.pop((channel.name, msg.uuid), None)
        if entry_time is None:  # start handler did not run (added mid-flight)
            return
        process_time = round(perf_counter() - entry_time, 6)

        for tagged in (msg, result):
            # `result` is None when the processing raised, and may be a
            # generator when the channel ends on a yielding node
            if isinstance(getattr(tagged, "meta", None), dict):
                tagged.meta[self.META_NAME] = process_time

        if msg.store_id and msg.store_chan_name == channel.short_name:
            await channel.message_store.add_message_meta_infos(
                msg.store_id, self.META_NAME, process_time)
