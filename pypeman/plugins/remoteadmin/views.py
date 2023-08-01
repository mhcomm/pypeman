import json

from aiohttp import web

from pypeman import channels


def get_channel(self, name):
    """
    return channel by is name.all_channels
    """
    for chan in channels.all_channels:
        if chan.name == name:
            return chan
    return None


async def list_channels(request):
    """
    Return a list of available channels.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chans = []
    print(channels.all_channels)
    for chan in channels.all_channels:
        if not chan.parent:
            chan_dict = chan.to_dict()
            print(chan_dict)
            chan_dict['subchannels'] = chan.subchannels()

            chans.append(chan_dict)

    resp_message = json.dumps(chans)
    await ws.send_str(resp_message)


async def start_channel(request, channelname):
    """
    Start the specified channel

    :params channel: The channel name to start.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)
    await chan.start()

    resp_message = json.dumps({
        'name': chan.name,
        'status': channels.BaseChannel.status_id_to_str(chan.status)
    })
    await ws.send_str(resp_message)


async def stop_channel(request, channelname):
    """
    Stop the specified channel

    :params channel: The channel name to stop.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)
    await chan.stop()

    resp_message = json.dumps({
        'name': chan.name,
        'status': channels.BaseChannel.status_id_to_str(chan.status)
    })
    await ws.send_str(resp_message)


async def list_msgs(request, channelname):
    """
    List first `count` messages from message store of specified channel.

    :params channel: The channel name.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)

    args = request.rel_url.query
    start = int(args.get("start", 0))
    count = int(args.get("count", 10))
    order_by = args.get("order_by", "timestamp")
    start_dt = args.get("start_dt", None)
    end_dt = args.get("end_dt", None)
    text = args.get("text", None)
    rtext = args.get("rtext", None)

    messages = await chan.message_store.search(
        start=start, count=count, order_by=order_by, start_dt=start_dt, end_dt=end_dt,
        text=text, rtext=rtext) or []

    for res in messages:
        res['timestamp'] = res['message'].timestamp_str()
        res['message'] = res['message'].to_json()

    resp_message = json.dumps({'messages': messages, 'total': await chan.message_store.total()})
    await ws.send_str(resp_message)


async def replay_msg(request, channelname, message_id):
    """
    Replay messages from message store.

    :params channel: The channel name.
    :params msg_ids: The message ids list to replay.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)
    result = []
    try:
        msg_res = await chan.replay(message_id)
        result.append(msg_res.to_dict())
    except Exception as exc:
        result.append({'error': str(exc)})

    resp_message = json.dumps(result)
    await ws.send_str(resp_message)


async def view_msg(request, channelname, message_id):
    """
    Permit to get the content of a message

    :params channel: The channel name.
    :params msg_ids: The message ids list to replay.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)
    result = []
    try:
        msg_res = await chan.message_store.get_msg_content(message_id)
        result.append(msg_res.to_dict())
    except Exception as exc:
        result.append({'error': str(exc)})

    resp_message = json.dumps(result)
    await ws.send_str(resp_message)


async def preview_msg(request, channelname, message_id):
    """
    Permits to get the 1000 chars of a message payload

    :params channel: The channel name.
    :params msg_ids: The message ids list to replay.
    """
    print(vars(request))
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    chan = get_channel(channelname)
    result = []
    try:
        msg_res = await chan.message_store.get_preview_str(message_id)
        result.append(msg_res.to_dict())
    except Exception as exc:
        result.append({'error': str(exc)})

    resp_message = json.dumps(result)
    await ws.send_str(resp_message)
