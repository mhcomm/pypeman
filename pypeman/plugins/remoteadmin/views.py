import json
import logging

from aiohttp import web
from jsonrpcserver.response import SuccessResponse

from pypeman import channels

logger = logging.getLogger(__name__)


def get_channel(name):
    """
    returns channel by name
    """
    for chan in channels.all_channels:
        if chan.name == name:
            return chan
    return None


async def list_channels(request, ws=None):
    """
    Returns a list of available channels.
    """

    chans = []
    for chan in channels.all_channels:
        if not chan.parent:
            chan_dict = chan.to_dict()
            chan_dict['subchannels'] = chan.subchannels()

            chans.append(chan_dict)

    if ws is not None:
        await ws.send_jsonrpcresp(chans)
        return ws
    return web.json_response(chans)


async def start_channel(request, ws=None):
    """
    Start the specified channel

    :params channelname: The channel name to start.
    """
    channelname = request.match_info['channelname']
    chan = get_channel(channelname)
    await chan.start()
    resp_dict = {
        'name': chan.name,
        'status': channels.BaseChannel.status_id_to_str(chan.status)
    }
    if ws is not None:
        await ws.send_jsonrpcresp(resp_dict)
        return ws
    return web.json_response(resp_dict)


async def stop_channel(request, ws=None):
    """
    Stop the specified channel

    :params channelname: The channel name to stop.
    """
    channelname = request.match_info['channelname']

    chan = get_channel(channelname)
    await chan.stop()

    resp_dict = {
        'name': chan.name,
        'status': channels.BaseChannel.status_id_to_str(chan.status)
    }
    if ws is not None:
        await ws.send_jsonrpcresp(resp_dict)
        return ws
    return web.json_response(resp_dict)


async def list_msgs(request, ws=None):
    """
    List first `count` message infos from message store of specified channel.
    Return list of dicts with the id, the status and the timestamp of the message
    without the content.

    :params channelname: The channel name.

    :queryparams:
        start (int, default=0): the start indexof msgs to list
        count (count, default=10): The maximum returned msgs
        order_by (str, default="timestamp"): the message attribute to use for sorting
        start_dt (str): iso datetime string to use for filter messages
        end_dt (str): iso datetime string to use for filter messages
        text (str): search this text in messages and return only messages that contains
                    this text
        rtext (str): same as 'text' param but for regex
    """

    channelname = request.match_info['channelname']

    chan = get_channel(channelname)

    args = request.rel_url.query
    start = int(args.get("start", 0))
    count = int(args.get("count", 10))
    order_by = args.get("order_by", "-timestamp")
    start_dt = args.get("start_dt", None)
    end_dt = args.get("end_dt", None)
    text = args.get("text", None)
    rtext = args.get("rtext", None)
    messages = await chan.message_store.search(
        start=start, count=count, order_by=order_by, start_dt=start_dt, end_dt=end_dt,
        text=text, rtext=rtext) or []

    for res in messages:
        res["id"] = res["id"]
        res["state"] = res.get("state", "UNKNOWN")
        res['timestamp'] = res['message'].timestamp_str()
        if "message" in res:
            res.pop("message")

    resp_dict = {'messages': messages, 'total': await chan.message_store.total()}
    if ws is not None:
        await ws.send_jsonrpcresp(resp_dict)
        return ws
    return web.json_response(resp_dict)


async def replay_msg(request, ws=None):
    """
    Replay message from message store.

    :params channel: The channel name.
    :params msg_id: The message id to replay.

    :queryparams:
        encode_payload (Bool, default=False): Force pickling and encoding the payload or not
    """
    channelname = request.match_info['channelname']
    message_id = request.match_info['message_id']
    args = request.rel_url.query
    encode_payload = args.get("encode_payload", False)
    chan = get_channel(channelname)
    try:
        msg_res = await chan.replay(message_id)
        result = msg_res.to_dict(encode_payload=encode_payload)
    except IndexError:
        message = f"Cannot replay msg, id {message_id} probably doesn't exists"
        logger.error(message)
        result = {'error': message}
    except Exception as exc:
        logger.exception(f"Cannot replay msg {message_id}")
        result = {'error': str(exc)}

    if ws is not None:
        await ws.send_jsonrpcresp(result)
        return ws
    return web.json_response(result)


async def view_msg(request, ws=None):
    """
    Permit to get the content of a message

    :params channelname: The channel name.
    :params message_id: The message id to view

    :queryparams:
        encode_payload (Bool, default=False): Force pickling and encoding the payload or not
    """

    channelname = request.match_info['channelname']
    message_id = request.match_info['message_id']

    args = request.rel_url.query
    encode_payload = args.get("encode_payload", False)

    chan = get_channel(channelname)

    try:
        msg_res = await chan.message_store.get_msg_content(message_id)
        result = msg_res.to_dict(encode_payload=encode_payload)
    except IndexError:
        message = f"Cannot view msg, id {message_id} probably doesn't exists"
        logger.error(message)
        result = {'error': message}
    except Exception as exc:
        logger.exception(f"Cannot view msg {message_id}")
        result = {'error': str(exc)}

    if ws is not None:
        await ws.send_jsonrpcresp(result)
        return ws
    return web.json_response(result)


async def preview_msg(request, ws=None):
    """
    Permits to get the first 1000 chars of a message payload

    :params channelname: The channel name.
    :params message_id: The message id to preview

    :queryparams:
        encode_payload (Bool, default=False): Force pickling and encoding the payload or not
    """
    channelname = request.match_info['channelname']
    message_id = request.match_info['message_id']

    args = request.rel_url.query
    encode_payload = args.get("encode_payload", False)

    chan = get_channel(channelname)
    try:
        msg_res = await chan.message_store.get_preview_str(message_id)
        result = msg_res.to_dict(encode_payload=encode_payload)
    except IndexError:
        message = f"Cannot preview msg, id {message_id} probably doesn't exists"
        logger.error(message)
        result = {'error': message}
    except Exception as exc:
        logger.exception(f"Cannot preview msg {message_id}")
        result = {'error': str(exc)}

    if ws is not None:
        await ws.send_jsonrpcresp(result)
        return ws
    return web.json_response(result)


class RPCWebSocketResponse(web.WebSocketResponse):
    """
    Mocked aiohttp.web.WebSocketResponse to return JSOn RPC responses
    Workaround to have a backport compatibility with old json rpc client
    """

    def set_rpc_attrs(self, request_data):
        self.rpc_data = request_data

    async def send_jsonrpcresp(self, message):
        message = SuccessResponse(message, id=self.rpc_data["id"])
        await super().send_str(str(message))


async def backport_old_client(request):
    ws = RPCWebSocketResponse()
    logger.debug("Receiving a request from the shell client (%r)", vars(request))
    await ws.prepare(request)
    async for msg in ws:
        try:
            cmd_data = json.loads(msg.data)
        except Exception:
            await ws.send_str(f"cannot parse ws json data ({msg.data})")
            return ws
        ws.set_rpc_attrs(cmd_data)
        cmd_method = cmd_data.pop("method")
        params = cmd_data.get("params", [None])
        channelname = params[0]
        if channelname:
            request.match_info["channelname"] = channelname

        if cmd_method == "channels":
            await list_channels(request, ws=ws)
        elif cmd_method == "preview_msg":
            message_id = params[1]
            request.match_info["message_id"] = message_id
            query_url = request.rel_url.with_query({"encode_payload": "True"})
            new_req = request.clone(rel_url=query_url)
            await preview_msg(request=new_req, ws=ws)
        elif cmd_method == "view_msg":
            message_id = params[1]
            request.match_info["message_id"] = message_id
            query_url = request.rel_url.with_query({"encode_payload": "True"})
            new_req = request.clone(rel_url=query_url)
            await view_msg(request=new_req, ws=ws)
        elif cmd_method == "replay_msg":
            message_id = params[1]
            request.match_info["message_id"] = message_id
            await replay_msg(request=request, ws=ws)
        elif cmd_method == "list_msgs":
            query_params = {
                "start": params[1],
                "count": params[2],
                "order_by": params[3],
                "start_dt": params[4],
                "end_dt": params[5],
                "text": params[6],
                "rtext": params[7],
            }
            query_params = {k: v for k, v in query_params.items() if v is not None}
            query_url = request.rel_url.with_query(query_params)
            new_req = request.clone(rel_url=query_url)
            await list_msgs(request=new_req, ws=ws)
        elif cmd_method == "start_channel":
            await start_channel(request=request, ws=ws)
        elif cmd_method == "stop_channel":
            await stop_channel(request=request, ws=ws)
        else:
            await ws.send_str(f"{cmd_method} is not a valid method")
