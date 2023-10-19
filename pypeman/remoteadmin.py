import asyncio
import cmd
import functools
import json
import logging
import re
import sys

from datetime import datetime
# TODO use readline for history ?
# import readline

import contextlib
import websockets

from aiohttp import web
from io import StringIO

from jsonrpcserver import async_dispatch
from jsonrpcserver import method
from jsonrpcclient import parse_json
from jsonrpcclient import request_json


from pypeman.conf import settings
from pypeman import channels
from pypeman import message

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def stdoutIO(stdout=None):
    old = sys.stdout
    if stdout is None:
        stdout = StringIO()
    sys.stdout = stdout
    yield stdout
    sys.stdout = old


class RemoteAdminServer():
    """
    Expose json/rpc function to a client by a websocket.
    """

    def __init__(self, loop=None, host='localhost', port='8091', ssl=None, url=None):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.loop = loop or asyncio.get_event_loop()
        self.ctx = {}

    def get_channel(self, name):
        """
        return channel by is name.all_channels
        """
        for chan in channels.all_channels:
            if chan.name == name:
                return chan
        return None

    async def start(self):
        """ Start remote admin server """

        start_server = websockets.serve(
            self.command,
            host=self.host,
            port=self.port,
            ssl=self.ssl,
            loop=self.loop
        )
        await start_server

    async def command(self, websocket, path):
        """
        Generic function to handle a command from client.
        """
        request = await websocket.recv()
        response = await async_dispatch(request, context=self)
        if response.wanted:
            await websocket.send(str(response))

    @method
    async def exec(self, command):
        """
        Execute a python command on this instance and
        return the stdout result.

        :param command: The python command to execute. Can be multiline.
        :returns: Command stdout result.
        """
        # TODO may cause problem on multi thread access to stdout
        # as we highjack sys.stdout
        with stdoutIO() as out:
            exec(command, globals())

        return out.getvalue()

    @method
    async def channels(self):
        """
        Return a list of available channels.
        """
        chans = []
        for chan in channels.all_channels:
            if not chan.parent:
                chan_dict = chan.to_dict()
                chan_dict['subchannels'] = chan.subchannels()

                chans.append(chan_dict)

        return chans

    @method
    async def start_channel(self, channel):
        """
        Start the specified channel

        :params channel: The channel name to start.
        """
        chan = self.get_channel(channel)
        await chan.start()
        return {
            'name': chan.name,
            'status': channels.BaseChannel.status_id_to_str(chan.status)
        }

    @method
    async def stop_channel(self, channel):
        """
        Stop the specified channel

        :params channel: The channel name to stop.
        """
        chan = self.get_channel(channel)
        await chan.stop()
        return {
            'name': chan.name,
            'status': channels.BaseChannel.status_id_to_str(chan.status)
        }

    @method
    async def list_msgs(self, channel, start=0, count=10, order_by='timestamp', start_dt=None, end_dt=None,
                        text=None, rtext=None):
        """
        List first `count` messages from message store of specified channel.

        :params channel: The channel name.
        """
        chan = self.get_channel(channel)

        messages = await chan.message_store.search(
            start=start, count=count, order_by=order_by, start_dt=start_dt, end_dt=end_dt,
            text=text, rtext=rtext) or []

        for res in messages:
            timestamp = res['timestamp']
            if isinstance(timestamp, datetime):
                timestamp = datetime.timestamp(timestamp)
            res['timestamp'] = timestamp
            res['id'] = res['id']
            res['state'] = res["state"]
            if "message" in res:
                res.pop("message")

        return {'messages': messages, 'total': await chan.message_store.total()}

    @method
    async def replay_msg(self, channel, msg_id):
        """
        Replay messages from message store.

        :params channel: The channel name.
        :params msg_ids: The message ids list to replay.
        """
        chan = self.get_channel(channel)
        try:
            msg_res = await chan.replay(msg_id)
            result = msg_res.to_dict()
        except Exception as exc:
            result = {'error': str(exc)}
        return result

    @method
    async def view_msg(self, channel, msg_id):
        """
        Permit to get the content of a message

        :params channel: The channel name.
        :params msg_ids: The message ids list to replay.
        """
        chan = self.get_channel(channel)
        try:
            msg_res = await chan.message_store.get_msg_content(msg_id)
            result = msg_res.to_dict()
        except Exception as exc:
            result = {'error': str(exc)}

        return result

    @method
    async def preview_msg(self, channel, msg_id):
        """
        Permits to get the 1000 chars of a message payload

        :params channel: The channel name.
        :params msg_ids: The message ids list to replay.
        """
        chan = self.get_channel(channel)
        try:
            msg_res = await chan.message_store.get_preview_str(msg_id)
            result = msg_res.to_dict()
        except Exception as exc:
            result = {'error': str(exc)}
        return result

    @method
    async def push_msg(self, channel, text):
        """
        Push a message in the channel.

        :params channel: The channel name.
        :params msg_ids: The text added to the payload.
        """
        chan = self.get_channel(channel)
        msg = message.Message(payload=text)
        result = await chan.handle(msg)
        return result.to_dict()


class RemoteAdminClient():
    """
    Remote admin client. To be use by ipython shell or pypeman shell.

    :params url: Pypeman Websocket url.
    """
    def __init__(self, loop=None, url='ws://localhost:8091'):
        self.url = url
        self.loop = loop or asyncio.get_event_loop()

    def init(self):
        pass

    def send_command(self, command, args=None):
        """
        Send a command to remote instance
        """
        if args is None:
            args = []
        if not isinstance(args, list):
            args = [args]
        return self.loop.run_until_complete(self._send_command(command, args))

    async def _send_command(self, command, args):
        """
        Asynchronous version of command sending
        """
        async with websockets.connect(self.url) as ws:
            await ws.send(request_json(command, args))
            response = await ws.recv()
            parsed_response = parse_json(response)
            return parsed_response.result

    def exec(self, command):
        """
        Execute any valid python code on remote instance
        and return stdout result.
        """
        result = self.send_command('exec', [command])
        return result

    def channels(self):
        """
        Return a list of available channels on remote instance.
        """
        return self.send_command('channels')

    def start(self, channel):
        """
        Start the specified channel on remote instance.

        :params channel: The channel name.
        """
        return self.send_command('start_channel', [channel])

    def stop(self, channel):
        """
        Stop the specified channel on remote instance.

        :params channel: The channel name.
        """
        return self.send_command('stop_channel', [channel])

    def list_msgs(self, channel, start=0, count=10, order_by='timestamp', start_dt=None, end_dt=None,
                  text=None, rtext=None):
        """
        List first 10 messages on specified channel from remote instance.

        :params channel: The channel name.
        :params start: Start index of listing.
        :params count: Count from index.
        :params start_dt: (optional) start datetime filter (isoformat)
        :params end_dt: (optional) start datetime filter (isoformat)
        :params text: (optional) text to search in message
        :params rtext: (optional) regex to search in message
        :params order_by: Message order. only 'timestamp' and '-timestamp' handled for now.
        :returns: list of message with status.
        """
        list_msg_args = [channel, start, count, order_by, start_dt, end_dt, text, rtext]
        result = self.send_command('list_msgs', list_msg_args)

        for m in result['messages']:
            if "message" in m:
                m.pop("message")

        return result

    def replay_msg(self, channel, msg_id):
        """
        Replay specified message from id list of specified channel on remote instance.

        :params channel: The channel name.
        :params msg_id: Message id to replay
        :returns: List of result for each message. Result
            can be {'error': <msg_error>} for one id if error
            occurs.
        """
        request_result = self.send_command('replay_msg', [channel, msg_id])

        if 'error' not in request_result:
            result = message.Message.from_dict(request_result)
        else:
            result = request_result

        return result

    def view_msg(self, channel, msg_id):
        """
        View content of the specified message from id of specified
        channel on remote instance.

        :params channel: The channel name.
        :params msg_ids: Message id list to replay
        :returns: List of result for each message. Result
            can be {'error': <msg_error>} for one id if error
            occurs.
        """
        request_result = self.send_command('view_msg', [channel, msg_id])

        if 'error' not in request_result:
            result = message.Message.from_dict(request_result)
        else:
            result = request_result

        return result

    def preview_msg(self, channel, msg_id):
        """
        Preview content of the specified message from id of specified
        channel on remote instance.

        :params channel: The channel name.
        :params msg_ids: Message id list to replay
        :returns: List of result for each message. Result
            can be {'error': <msg_error>} for one id if error
            occurs.
        """
        request_result = self.send_command('preview_msg', [channel, msg_id])

        if 'error' not in request_result:
            result = message.Message.from_dict(request_result)
        else:
            result = request_result

        return result

    def push_msg(self, channel, text):
        """
        Push a new message from text param to the channel.

        :params channel: The channel name.
        :params text: This text will be the payload of the message.
        """
        msg_dict = self.send_command('push_msg', [channel, text])
        return message.Message.from_dict(msg_dict)


def _with_current_channel(func):
    """
    Decorator to select channel for command.
    """
    @functools.wraps(func)
    def wrapper(self, *arg, **kwargs):
        if self.current_channel:
            return func(self, self.current_channel, *arg, **kwargs)
        else:
            print("You have to select a channel prior using this command")
            return None

    return wrapper


class PypemanShell(cmd.Cmd):
    intro = 'Welcome to the pypeman shell. Type help or ? to list commands.\n'
    prompt = 'pypeman > '
    use_rawinput = False

    def __init__(self, url):
        super().__init__()
        self.current_channel = None
        self.client = RemoteAdminClient(url=url)
        self.client.init()

    def do_channels(self, arg):
        "List avaible channels"
        result = self.client.channels()
        print("\nChannel list:")
        for idx, channel in enumerate(result):
            print("{idx}) {name} ({status})".format(idx=idx, **channel))

        print("")
        self.current_channel = None
        self.prompt = "pypeman > "

    def do_stop(self, arg):
        "Stop a channel by his name"
        result = self.client.stop(arg)
        print(result)

    def do_start(self, arg):
        "Start a channel by his name"
        result = self.client.start(arg)
        print(result)

    def do_select(self, arg):
        """
        Select a channel by is name.
        Mandatory for channel oriented command
        """
        self.current_channel = arg
        self.prompt = "pypeman(%s) > " % arg

    def get_current_channel(self):
        if self.current_channel:
            return self.current_channel
        else:
            print("You have to select a channel before for this command")
            return None

    @_with_current_channel
    def do_list(self, channel, arg):
        """
        List messages of selected channel. You can specify start, end and order_by arguments
        Optional args:
            - to filter messages, you can pass start_dt and end_dt (isoformat datetimes)
            to filter messages
            - to filter messages, you can pass text and rtext (string between double quotes)
            to filter messages
        """
        dquote_args_regex = r'\w+=".*?"'
        dquote_args = re.findall(dquote_args_regex, arg)
        for st_arg in dquote_args:
            arg = arg.replace(st_arg, "")
        dquote_args = [dqarg.replace('"', "") for dqarg in dquote_args]
        args = arg.split()
        if dquote_args:
            args.extend(dquote_args)
        start, end, order_by = 0, 100, '-timestamp'
        start_dt = None
        end_dt = None
        text = None
        rtext = None

        args_copy = [i for i in args]
        # Parsing of naming args
        for arg in args_copy:
            if isinstance(arg, str):
                if arg.startswith("start_dt="):
                    start_dt = arg.split("=")[1]
                    args.remove(arg)
                if arg.startswith("end_dt="):
                    end_dt = arg.split("=")[1]
                    args.remove(arg)
                if arg.startswith("text="):
                    text = arg.split("=", 1)[1]
                    args.remove(arg)
                if arg.startswith("rtext="):
                    rtext = arg.split("=", 1)[1]
                    args.remove(arg)

        # Parsing of common args
        if args:
            start = int(args[0])
        if len(args) > 1:
            end = int(args[1])
        if len(args) > 2:
            order_by = args[2]
        result = self.client.list_msgs(
            channel, start, end, order_by, start_dt=start_dt, end_dt=end_dt,
            text=text, rtext=rtext)

        if not result['total']:
            print('No message yet.')

        for res in result['messages']:
            print(res['timestamp'], res['id'], res['state'])

    @_with_current_channel
    def do_replay(self, channel, arg):
        "Replay a message list by their ids"
        msg_ids = str(arg).split()
        for msg_id in msg_ids:
            msg = self.client.replay_msg(channel, msg_id)
            print("Result message for replaying message %s:" % msg_id)
            if isinstance(msg, message.Message):
                print(msg.to_print())
            else:
                print('Error on msg: %s - %s' % (msg_id, msg['error']))

    @_with_current_channel
    def do_view(self, channel, arg):
        "View content of a message list by their ids"
        msg_ids = arg.split()
        for msg_id in msg_ids:
            msg = self.client.view_msg(channel, msg_id)
            print("View of msg %s:" % msg_id)
            if isinstance(msg, message.Message):
                print(msg.to_print())
            else:
                print('Error on msg: %s - %s' % (msg_id, msg['error']))

    @_with_current_channel
    def do_preview(self, channel, arg):
        "Preview content of a message list by their ids"
        msg_ids = arg.split()
        for msg_id in msg_ids:
            msg = self.client.preview_msg(channel, msg_id)
            print("Preview of message %s:" % msg_id)
            if isinstance(msg, message.Message):
                print(msg.to_print())
            else:
                print('Error on msg: %s - %s' % (msg_id, msg['error']))

    @_with_current_channel
    def do_push(self, channel, arg):
        "Inject message with text as payload for selected channel"
        result = self.client.push_msg(channel, arg)
        print("Result message:")
        print(result.to_print())

    def do_exit(self, arg):
        "Exit program"
        sys.exit()


class WebAdmin():
    def __init__(self, loop, host, port, ssl):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.loop = loop

    async def start(self):

        # client_dir = os.path.join(os.path.dirname(os.path.join(__file__)), 'client/dist')
        app = web.Application()
        app.router.add_get(
            '/configs.js',
            self.handle_config
        )

        # redirect to index.html
        app.router.add_get('/', self.redirect_to_index)

        # app.router.add_static(
        #     '/',
        #     path=os.path.join(client_dir),
        #     name='static'
        # )

        await self.loop.create_server(
            protocol_factory=app.make_handler(),
            host=self.host,
            port=self.port,
            ssl=self.ssl
        )

    async def redirect_to_index(self, request):
        """ redirect to index.html"""
        return web.HTTPFound("index.html")

    async def handle_config(self, request):
        conf = settings.REMOTE_ADMIN_WEBSOCKET_CONFIG
        server_url = conf['url'] or "ws{is_secure}://{host}:{port}".format(
            is_secure='s' if conf['ssl'] else '',
            **conf
        )

        conf = {
            'serverConfig': server_url
        }
        # TODO Not really cool but works.
        resp = """window.configs = {};""".format(json.dumps(conf))

        return web.Response(text=resp)
