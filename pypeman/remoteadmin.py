import sys
import asyncio
import logging
import json
import cmd
import itertools
import functools
# TODO use readline for history ?
#import readline

import websockets
from jsonrpcserver.aio import methods
from jsonrpcserver.response import NotificationResponse
from jsonrpcclient.websockets_client import WebSocketsClient
from jsonrpcclient.exceptions import ReceivedErrorResponse

from pypeman.conf import settings
from pypeman import channels
from pypeman import message

logger = logging.getLogger(__name__)

from io import StringIO
import contextlib

@contextlib.contextmanager
def stdoutIO(stdout=None):
    old = sys.stdout
    if stdout is None:
        stdout = StringIO()
    sys.stdout = stdout
    yield stdout
    sys.stdout = old

class RemoteAdminServer():

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.ctx = {}

    def get_channel(self, name):
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
        methods.add(self.push_msg)

        start_server = websockets.serve(
            self.command,
            self.host,
            self.port
        )
        await start_server

    async def command(self, websocket, path):
        request = await websocket.recv()
        response = await methods.dispatch(request)
        if not response.is_notification:
            await websocket.send(str(response))

    async def exec(self, command):
        # TODO may cause problem on multi thread access to stdout
        # as we highjack sys.stdout
        with stdoutIO() as out:
            exec(command, globals())

        return out.getvalue()

    async def channels(self):
        chans = []
        for chan in channels.all:
            if not chan.parent:
                chans.append({
                    'name': chan.name,
                    'status': channels.BaseChannel.status_id_to_str(chan.status)
                })
        return chans

    async def start_channel(self, channel):
        chan = self.get_channel(channel)
        await chan.start()

    async def stop_channel(self, channel):
        chan = self.get_channel(channel)
        await chan.start()

    async def list_msg(self, channel):
        chan = self.get_channel(channel)
        result = list(itertools.islice(chan.message_store.search(), 10))
        return result

    async def push_msg(self, channel, text):
        chan = self.get_channel(channel)
        msg = message.Message(payload=text)
        result = await chan.handle(msg)
        return result.to_dict()


class RemoteAdminClient():
    def __init__(self, url):
        self.url = url
        self.loop = asyncio.get_event_loop()

    def init(self):
        pass

    def send_command(self, command, args=None):
        if args is None:
            args = []
        return self.loop.run_until_complete(self._send_command(command, args))

    async def _send_command(self, command, args):
        async with websockets.connect(self.url) as ws:
            response = await WebSocketsClient(ws).request(command, *args)
            return response

    def exec(self, command):
        """ Execute any python valid code """
        result = self.send_command('exec', [command])
        print(result)
        return

    def channels(self):
        return self.send_command('channels')

    def start(self, channel):
        return self.send_command('start_channel', [channel])

    def stop(self, channel):
        return self.send_command('stop_channel', [channel])

    def list_msg(self, channel):
        return self.send_command('list_msg', [channel])

    def push_msg(self, channel, text):
        return self.send_command('push_msg', [channel, text])


def _with_current_channel(func):
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
        self.client = RemoteAdminClient(url)
        self.client.init()

    def do_channels(self, arg):
        "List avaible channels"
        result = self.client.channels()
        for channel in result:
            print("{name} ({status})".format(**channel))

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
    def do_listmsg(self, channel, arg):
        "List 10 messages of selected channel"
        result = self.client.list_msg(channel)
        print(result)

    @_with_current_channel
    def do_push(self, channel, arg):
        "Inject message from text for selected channel"
        result = self.client.push_msg(channel, arg)
        print(result)

    def do_exit(self, arg):
        "Exit program"
        sys.exit(0)

