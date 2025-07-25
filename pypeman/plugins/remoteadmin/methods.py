from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING
from typing import Awaitable
from typing import Callable
from typing import TypedDict
from typing import Literal

from aiohttp import ClientWebSocketResponse
from aiohttp import web

from ...channels import BaseChannel
from ...channels import all_channels
from ...channels import get_channel

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

        For consistency again, ecorated functions must only take
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
