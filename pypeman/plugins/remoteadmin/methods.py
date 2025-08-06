from __future__ import annotations

import json
from datetime import datetime
from functools import wraps
from typing import TYPE_CHECKING
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Literal
from typing import TypedDict

from aiohttp import ClientWebSocketResponse
from aiohttp import web
from dateutil import parser as dateutilparser

from ...channels import BaseChannel
from ...channels import all_channels
from ...channels import get_channel
from ...message import Message

if TYPE_CHECKING:
    # ParamSpec doesn't exit before 3.10;
    # if you're here after that, you can merge the branches by
    # moving the implementation into the typed version
    from typing import ParamSpec
    from typing import TypeVar
    from typing import overload

    _R_ = TypeVar("_R_")
    _P_ = ParamSpec("_P_")

    def _remote_proc(_rfn: Callable[_P_, Awaitable[_R_]]):
        """Make a function into a compatible remote procedure.

        This means it get 2 new overloads on top of the normal call:
            * the first one makes it a compatible aiohttp URL handler;
            * the second one makes it perform a RPC through websocket.

        Each of these are used in difference places that needs to
        access the exact same functionality. This makes it consistent
        accros these places:
            * aiohttp URL handler;
            * remoteadmin CLI (both client- and server- side);
            * normal function call.

        For consistency again, decorated functions must only take
        keyword arguments:

            @_remote_proc
            async def hello(*, world): ...
        """

        @overload
        async def fn(req: web.Request) -> web.Response:
            """called from aiohttp registered url"""

        @overload
        async def fn(ws: ClientWebSocketResponse, *_: _P_.args, **kwargs: _P_.kwargs) -> _R_:
            """called from remoteadmin CLI (websocket client-side)"""

        @overload
        async def fn(*_: _P_.args, **kwargs: _P_.kwargs) -> _R_:
            """called from websocket server-side (regular call)"""

        async def fn(*_a: ..., **_ka: ...) -> ...: ...

        return fn

else:

    def _remote_proc(rfn):
        """Make a function into a compatible remote procedure.

        This means it get 2 new overloads on top of the normal call:
            * the first one makes it a compatible aiohttp URL handler;
            * the second one makes it perform a RPC through websocket.

        Each of these are used in difference places that needs to
        access the exact same functionality. This makes it consistent
        accros these places:
            * aiohttp URL handler;
            * remoteadmin CLI (both client- and server- side);
            * normal function call.

        For consistency again, decorated functions must only take
        keyword arguments:

            @_remote_proc
            async def hello(*, world): ...
        """

        @wraps(rfn)
        async def fn(maybe_req_or_ws=None, /, **kwargs):
            # called from aiohttp registered url
            if isinstance(maybe_req_or_ws, web.Request):
                req = maybe_req_or_ws
                res = await rfn(**req.match_info, **req.rel_url.query)
                return web.json_response(res)

            # called from remoteadmin CLI (websocket client-side)
            if isinstance(maybe_req_or_ws, ClientWebSocketResponse):
                ws = maybe_req_or_ws
                await ws.send_json({"method": rfn.__name__, "params": kwargs})
                res = await ws.receive_json()
                if "error" in res:
                    raise RuntimeError(res["error"]["message"])
                return res["result"]

            # called from aiohttp websocket server-side
            # (or any other regular call uses)
            return await rfn(**kwargs)

        fn.is_remote_proc = True
        return fn


class ChannelAsDict_(TypedDict):
    name: str
    short_name: str
    verbose_name: str
    status: Literal["STARTING", "WAITING", "PROCESSING", "STOPPING", "STOPPED", "PAUSED"]
    has_message_store: bool
    processed: int
    subchannels: list[ChannelAsDict_]


@_remote_proc
async def list_channels() -> list[ChannelAsDict_]:
    chans = []
    for chan in all_channels:
        chan_dict = chan.to_dict()
        if chan_dict["has_message_store"]:
            chan_dict["subchannels"] = chan.subchannels()
        chans.append(chan_dict)
    return chans


class StartStopChannel_(TypedDict):
    name: str
    status: Literal["STARTING", "WAITING", "PROCESSING", "STOPPING", "STOPPED", "PAUSED"]


@_remote_proc
async def start_channel(*, channelname: str) -> StartStopChannel_:
    """Start the specified channel.

    :params channelname: Name of the channel to start.
    """
    chan = get_channel(channelname)
    assert chan, "channel not found"
    await chan.start()
    return {
        "name": chan.name,
        "status": BaseChannel.status_id_to_str(chan.status),
    }


@_remote_proc
async def stop_channel(*, channelname: str) -> StartStopChannel_:
    """
    Stop the specified channel

    :params channelname: The channel name to stop.
    """
    chan = get_channel(channelname)
    assert chan, "channel not found"
    await chan.stop()
    return {
        "name": chan.name,
        "status": BaseChannel.status_id_to_str(chan.status),
    }


# TODO(MR-local): remove once #322
class MessageStore_StoredEntry_(TypedDict):
    id: str  # store-dependant id, different from message.uuid
    meta: dict[str, list[str]]  # store-related meta, different from message.meta
    message: Message
    state: Message.State_  # TODO: remove in favor of _['meta']['state']


class ListMsgsItem_(TypedDict):
    id: str
    meta: dict[str, list[str]]
    timestamp: str  # "%Y-%m-%dT%H:%M:%S.%fZ" -- could/should be changed to an actual timestamp
    # TODO(MR-local): use `Message.State_` once #322
    state: Literal["pending", "processing", "error", "rejected", "processed", "UNKNOWN"]


class ListMsgs_(TypedDict):
    messages: list[ListMsgsItem_] | dict[str, list[ListMsgsItem_]]
    total: int


def _entries_to_listmsg_res(entries: list[MessageStore_StoredEntry_]) -> list[ListMsgsItem_]:
    "use by list_msgs to transpose search result to JSON-sendable ones"
    return [
        {
            "id": entry["id"],
            "meta": entry["meta"],
            "timestamp": entry["message"].timestamp_str(),  # tag: not an actual timestamp...
            "state": entry["state"],
        }
        for entry in entries
    ]


@_remote_proc
async def list_msgs(*, channelname: str, **search_kwargs: str) -> ListMsgs_:
    """List and search through messages in a channel's store.

    This is a direct call to :meth:`MessageStore.search`. The returned
    object differ in that it does not contain the messages themselves,
    making it lighter and easier to JSON-serialize.

    Like other remote procedure, all arguments are plain str and thus
    'meta'-based searches must use `'meta_..'`.
    """
    # validate and convert arguments as needed,
    # at the same time collect the meta-based args
    kwargs: dict[str, str | int | datetime] = {}
    metargs: dict[str, str] = {}

    for name, val in search_kwargs.items():
        if name == "count":
            val = int(val)
        elif name == "order_by":
            if val not in {"timestamp", "state", "-timestamp", "-state"}:
                raise ValueError(f"`order_by` cannot be {val!r}")
        elif name == "group_by":
            if val not in {"state"}:
                raise ValueError(f"`group_by` cannot be {val!r}")
        elif name in {"start_dt", "end_dt"}:
            val = dateutilparser.isoparse(val)

        elif name.startswith("meta_"):
            metargs[name[5:]] = val
            continue  # don't add it to kwargs..

        kwargs[name] = val

    chan = get_channel(channelname)
    assert chan, "channel not found"

    found: ... = await chan.message_store.search(**kwargs, meta=metargs)
    # TODO(MR-local): annotations above and below unnecessary once #322
    # (well truthfully, it would also need hinting in `get_channel` and `BaseChannel`)
    found: list[MessageStore_StoredEntry_] | dict[str, list[MessageStore_StoredEntry_]]
    return {
        "messages": (
            {group: _entries_to_listmsg_res(found[group]) for group in found}
            if isinstance(found, dict)  # otherwise it's a list
            else _entries_to_listmsg_res(found)
        ),
        "total": await chan.message_store.total(),
    }


# guu, proper type for it 's not comming before a while
Message_AsDict_ = Any


@_remote_proc
async def replay_msg(*, channelname: str, message_id: str) -> Message_AsDict_:
    """Replay a message through the given channel.

    The message must of course exist in the channel's store.

    :return: see :meth:`Message.to_dict`
    """
    chan = get_channel(channelname)
    assert chan, "channel not found"

    msg_res = await chan.replay(message_id)
    return msg_res.to_dict()


@_remote_proc
async def view_msg(*, channelname: str, message_id: str) -> Message_AsDict_:
    """Retrieve a message from the given channel.

    The message must of course exist in the channel's store.

    :return: see :meth:`Message.to_dict`
    """
    chan = get_channel(channelname)
    assert chan, "channel not found"

    msg_res = await chan.message_store.get(message_id)["message"]
    return msg_res.to_dict()


@_remote_proc
async def push_msg(*, channelname: str, payload: str, meta: str) -> Message_AsDict_:
    """Push a message for the channel to handle.

    `meta`, if not empty, is expected to be a JSON object (ie python
    dict).

    :return: see :meth:`Message.to_dict`
    """
    chan = get_channel(channelname)
    assert chan, "channel not found"

    result = await chan.handle(Message(payload=payload, meta=json.loads(meta) if meta else None))
    return result.to_dict()
