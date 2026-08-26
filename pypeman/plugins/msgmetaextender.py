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
        return len(payload.encode("utf-8"))
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
    in (the store got its copy before most of them were known):

    * `process_time` — channel processing duration in seconds, also
      written to `msg.meta` of the entry and returned messages;
    * `input_size` / `input_type` — byte size (see :func:`payload_size`)
      and type name of the payload at channel entry, also written to
      `msg.meta` of the entry message before it is stored/processed;
    * `content_type` — the message's content type at channel entry
      (also in `msg.meta`, same as the input metas);
    * `output_size` / `output_type` — same as input for the payload of
      the returned message (absent when the processing raised or
      returned a generator);
    * `ctx_size` — total byte size of the payloads saved in the message
      context (`msg.add_context`) during processing, unmeasurable ones
      ignored.

    A message forked or routed to a subchannel is tagged once per
    channel it goes through, the innermost channel being the one whose
    values end up in the returned message.

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
        if isinstance(msg.meta, dict):
            # fired before the store copy: seen by the nodes and stored
            msg.meta.update(entry_metas)
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

        for tagged in (msg, result):
            if isinstance(getattr(tagged, "meta", None), dict):
                tagged.meta[self.META_PROCESS_TIME] = process_time

        if msg.store_id and msg.store_chan_name == channel.short_name:
            await channel.message_store.update_message_meta_infos(msg.store_id, metas)
