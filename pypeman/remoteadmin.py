import asyncio
import cmd
import functools
import json
import logging
import os
import sys

# TODO use readline for history ?
# import readline

import contextlib
import websockets

from aiohttp import web
from io import StringIO

from jsonrpcserver.aio import methods
from jsonrpcclient.websockets_client import WebSocketsClient


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
        return channel by is name.all
        """
        for chan in channels.all:
            if chan.name == name:
                return chan
        return None

    async def start(self):
        """ Start remote admin server """
        methods.add(self.exec)
        methods.add(self.channels)
        methods.add(self.stop_channel)
        methods.add(self.start_channel)
        methods.add(self.list_msg)
        methods.add(self.replay_msg)
        methods.add(self.push_msg)

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
        response = await methods.dispatch(request)
        if not response.is_notification:
            await websocket.send(str(response))

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

    async def channels(self):
        """
        Return a list of available channels.
        """
        chans = []
        for chan in channels.all:
            if not chan.parent:
                chan_dict = chan.to_dict()
                chan_dict['subchannels'] = chan.subchannels()

                chans.append(chan_dict)

        return chans

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

    async def list_msg(self, channel, start=0, count=10, order_by='timestamp'):
        """
        List first `count` messages from message store of specified channel.

        :params channel: The channel name.
        """
        chan = self.get_channel(channel)

        messages = await chan.message_store.search(start=start, count=count, order_by=order_by)

        for res in messages:
            res['timestamp'] = res['message'].timestamp_str()
            res['message'] = res['message'].to_json()

        return {'messages': messages, 'total': await chan.message_store.total()}

    async def replay_msg(self, channel, msg_ids):
        """
        Replay messages from message store.

        :params channel: The channel name.
        :params msg_ids: The message ids list to replay.
        """
        chan = self.get_channel(channel)
        result = []
        for msg_id in msg_ids:
            try:
                msg_res = await chan.replay(msg_id)
                result.append(msg_res.to_dict())
            except Exception as exc:
                result.append({'error': str(exc)})

        return result

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
        return self.loop.run_until_complete(self._send_command(command, args))

    async def _send_command(self, command, args):
        """
        Asynchronous version of command sending
        """
        async with websockets.connect(self.url, loop=self.loop) as ws:
            response = await WebSocketsClient(ws).request(command, *args)
            return response

    def exec(self, command):
        """
        Execute any valid python code on remote instance
        and return stdout result.
        """
        result = self.send_command('exec', [command])
        print(result)
        return

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

    def list_msg(self, channel, start=0, count=10, order_by='timestamp'):
        """
        List first 10 messages on specified channel from remote instance.

        :params channel: The channel name.
        :params start: Start index of listing.
        :params count: Count from index.
        :params order_by: Message order. only 'timestamp' and '-timestamp' handled for now.
        :returns: list of message with status.
        """
        result = self.send_command('list_msg', [channel, start, count, order_by])

        for m in result['messages']:
            m['message'] = message.Message.from_json(m['message'])

        return result

    def replay_msg(self, channel, msg_ids):
        """
        Replay specified message from id list of specified channel on remote instance.

        :params channel: The channel name.
        :params msg_ids: Message id list to replay
        :returns: List of result for each message. Result
            can be {'error': <msg_error>} for one id if error
            occurs.
        """
        request_result = self.send_command('replay_msg', [channel, msg_ids])

        result = []
        for msg in request_result:
            if 'error' not in msg:
                result.append(message.Message.from_dict(msg))
            else:
                result.append(msg)

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
        result = self.client.stop_channel(arg)
        print(result)

    def do_start(self, arg):
        "Start a channel by his name"
        result = self.client.start_channel(arg)
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
        "List messages of selected channel. You can specify start, end and order_by arguments"
        args = arg.split()
        start, end, order_by = 0, 10, '-timestamp'

        if args:
            start = args[0]
        if len(args) > 1:
            start = args[1]
        if len(args) > 2:
            start = args[2]

        result = self.client.list_msg(channel, start, end, order_by)

        if not result['total']:
            print('No message yet.')

        for res in result['messages']:
            print(res['message'].timestamp, res['id'], res['state'])

    @_with_current_channel
    def do_replay(self, channel, arg):
        "Replay a message list by their ids"
        msg_ids = arg.split()
        results = self.client.replay_msg(channel, msg_ids)
        for msg_id, msg in zip(msg_ids, results):
            print("Result message for replaying message %s:" % msg_id)
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

        client_dir = os.path.join(os.path.dirname(os.path.join(__file__)), 'client/dist')
        app = web.Application()
        app.router.add_get(
            '/configs.js',
            self.handle_config
        )

        # redirect to index.html
        app.router.add_get('/', self.redirect_to_index)

        app.router.add_static(
            '/',
            path=os.path.join(client_dir),
            name='static'
        )

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
