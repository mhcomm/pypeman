"""Plugin extending the message metas with processing facts.

Enabled by default; deactivate it by adding
`"pypeman.plugins.msgmetaextender.MsgMetaExtenderPlugin"` to
`settings.DISABLED_PLUGINS`.
"""

from __future__ import annotations

from time import perf_counter

from pypeman import events
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import TaskPluginMixin


def payload_size(payload):
    """Byte size of a payload, or None when it has no cheap byte size.

    Sizing an arbitrary object would mean serializing it (misleading
    for a repr, costly for a pickle), so only str/bytes-like payloads
    are measured, a str by the byte length of its UTF-8 encoding.
    """
    if isinstance(payload, (bytes, bytearray)):
        return len(payload)
    if isinstance(payload, memoryview):
        return payload.nbytes
    if isinstance(payload, str):
        if payload.isascii():  # C-speed scan, no allocation
            return len(payload)
        # exact utf-8 size without allocating a full-size copy: slicing a
        # str cannot split a code point, so the chunked sum is exact
        return sum(len(payload[i:i + 2 ** 16].encode("utf-8"))
                   for i in range(0, len(payload), 2 ** 16))
    return None


def context_size(ctx):
    """Total byte size of the payloads saved in a message context.

    Payloads with no cheap byte size are ignored.
    """
    total = 0
    for entry in ctx.values():
        size = payload_size(entry.get("payload")) if isinstance(entry, dict) else None
        if size is not None:
            total += size
    return total


class MsgMetaExtenderPlugin(BasePlugin, TaskPluginMixin):
    """Tag every message with facts about its processing by a channel.

    Written to the message store entry of the message the channel took
    in, and only there: the metas never touch `msg.meta`, so they cannot
    reach the nodes nor the payloads they build.

    * `process_time` — channel processing duration in seconds;
    * `input_size` / `input_type` — byte size (see :func:`payload_size`)
      and type name of the payload at channel entry;
    * `content_type` — the message's content type at channel entry;
    * `output_size` / `output_type` — same as input for the payload of
      the returned message (absent when the processing raised or
      returned a generator);
    * `ctx_size` — total byte size of the payloads saved in the message
      context (`msg.add_context`) during processing, unmeasurable ones
      ignored.

    Reserved store-meta keys: the seven names above are written to the
    store entry by this plugin, so a project must not use any of them as
    a node `store_meta` key.

    A message forked or routed to a subchannel is tagged once per
    channel it goes through, each channel writing to its own store
    entry.

    This plugin doubles as a reference for :mod:`pypeman.events` based
    plugins: subscribe in `task_start`, unsubscribe in `task_stop`.
    """

    META_PROCESS_TIME = "process_time"
    META_INPUT_SIZE = "input_size"
    META_INPUT_TYPE = "input_type"
    META_OUTPUT_SIZE = "output_size"
    META_OUTPUT_TYPE = "output_type"
    META_CONTENT_TYPE = "content_type"
    META_CTX_SIZE = "ctx_size"

    def __init__(self):
        # (channel name, message uuid) -> (entry time, entry metas); a
        # message keeps its uuid across channels, a channel handles it
        # only once
        self._inflight: dict[tuple[str, str], tuple[float, dict]] = {}

    async def task_start(self):
        events.msg_processing_start.add_handler(self._on_start)
        events.msg_processing_end.add_handler(self._on_end)

    async def task_stop(self):
        events.msg_processing_start.remove_handler(self._on_start)
        events.msg_processing_end.remove_handler(self._on_end)

    async def _on_start(self, channel, msg):
        # measured at entry, before the nodes mutate the payload
        entry_metas = {self.META_INPUT_TYPE: type(msg.payload).__name__}
        input_size = payload_size(msg.payload)
        if input_size is not None:
            entry_metas[self.META_INPUT_SIZE] = input_size
        content_type = getattr(msg, "content_type", None)
        if content_type is not None:
            entry_metas[self.META_CONTENT_TYPE] = content_type
        self._inflight[(channel.name, msg.uuid)] = (perf_counter(), entry_metas)

    async def _on_end(self, channel, msg, result, exc):
        inflight = self._inflight.pop((channel.name, msg.uuid), None)
        if inflight is None:  # start handler did not run (added mid-flight)
            return
        entry_time, entry_metas = inflight
        process_time = round(perf_counter() - entry_time, 6)

        metas = dict(entry_metas)
        metas[self.META_PROCESS_TIME] = process_time
        # `result` is None when the processing raised, and may be a
        # generator when the channel ends on a yielding node
        if isinstance(getattr(result, "meta", None), dict):
            metas[self.META_OUTPUT_TYPE] = type(result.payload).__name__
            output_size = payload_size(result.payload)
            if output_size is not None:
                metas[self.META_OUTPUT_SIZE] = output_size

        # contexts accumulate during processing, so measure them at exit
        ctx = getattr(result, "ctx", None)
        if not isinstance(ctx, dict):
            ctx = getattr(msg, "ctx", None)
        if isinstance(ctx, dict):
            metas[self.META_CTX_SIZE] = context_size(ctx)

        if msg.store_id and msg.store_chan_name == channel.short_name:
            await channel.message_store.update_message_meta_infos(msg.store_id, metas)
