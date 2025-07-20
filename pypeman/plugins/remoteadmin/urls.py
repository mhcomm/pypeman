"""URL exposed by the remoteadmin web server plugin task.

:func:`init_urls` adds the request handlers that re-expose the remote
methods from :mod:`methods` in two ways:
    * normal direct HTTP(s) (GET only) endpoints;
    * a websocket-based endpoint which :mod:`shell` delegates to.

The websocket part is handled by the private (but mentionned for doc
purpose) :func:`_rpc_url_handler` which implements WS JSON RPC server.
"""

import inspect
import json
from logging import getLogger

from aiohttp import WSMsgType
from aiohttp import web

from . import methods

logger = getLogger(__name__)


async def _rpc_url_handler(request: web.Request):
    """Glue URL handler / websocket sever.

    This connects between websocket requests (in the form of JSON RPC)
    from :mod:`shell` and :mod:`methods`.

    To establish a connection, a websocket client must request at the
    root (or prefix) URL allocated to the remoteadmin.

    Each JSON RPC request **must** present the parameter structure
    (`'params'` field) which **must** be using the "by-name" passing
    convention (ie be a JSON object / Python `dict`).
    See https://www.jsonrpc.org/specification section 4.2 #answer-to-everythin.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("websocket connection established")

    async def err(code: int, message: str, data: ..., exc: BaseException | None = None):
        """log + answer with error
        if exc is given, data must be a dict, cause we'll add ['error']
        """
        logger.error(f"(in rpc processing) {message} %s", data, exc_info=exc)
        if exc is not None:
            data["error"] = str(exc)
        await ws.send_json({"error": {"code": code, "message": message, "data": data}})

    async for msg in ws:
        if WSMsgType.TEXT == msg.type:
            try:
                rpc = json.loads(msg.data)
            except json.JSONDecodeError as e:
                await err(-32700, "Parse error", {"pos": e.pos, "lineno": e.lineno, "colno": e.colno})
                continue

            if "method" not in rpc or "params" not in rpc:
                await err(-32600, "Invalid Request", f"no {'params' if 'method' in rpc else 'method'!r}")
                continue

            rfn = getattr(methods, rpc["method"], None)
            if rfn is None or getattr(rfn, "is_remote_proc", False) is not True:
                await err(-32601, "Method not found", f"no method {rpc['method']!r}")
                continue

            params = rpc["params"]
            expects = inspect.getfullargspec(rfn).kwonlyargs
            if not isinstance(params, dict) or set(expects) < params.keys():
                await err(-32602, "Invalid params", {"given": params, "expects": expects})
                continue

            try:
                res = await rfn(**params)
            except BaseException as e:
                await err(-32603, f"Internal error in {rpc['method']}", {"params": params}, e)
                continue
            await ws.send_json({"result": res})

        elif WSMsgType.ERROR == msg.type:
            logger.warning("websocket connection closed with exception %s", ws.exception())

    logger.info("websocket connection closed")
    return ws


def init_urls(app: web.Application, prefix: str):
    """Create the pypeman remoteadmin routing.

    Please see the module-level documentation for actual detail.
    """
    app.add_routes(
        [
            # web API:
            web.get(prefix + "/channels", methods.list_channels),
            web.get(prefix + "/channels/{channelname}/start", methods.start_channel),
            web.get(prefix + "/channels/{channelname}/stop", methods.stop_channel),
            web.get(prefix + "/channels/{channelname}/messages", methods.list_msgs),
            web.get(prefix + "/channels/{channelname}/messages/{message_id}/replay", methods.replay_msg),
            web.get(prefix + "/channels/{channelname}/messages/{message_id}/view", methods.view_msg),
            web.get(prefix + "/channels/{channelname}/messages/{message_id}/preview", methods.view_msg),
            # websocket:
            web.get(prefix + "/", _rpc_url_handler),
        ]
    )
