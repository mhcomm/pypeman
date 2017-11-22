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
    """
    Expose json/rpc function to a client by a websocket.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
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
            self.host,
            self.port
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
                chans.append({
                    'name': chan.name,
                    'status': channels.BaseChannel.status_id_to_str(chan.status)
                })
        return chans

    async def start_channel(self, channel):
        """
        Start the specified channel

        :params channel: The channel name to start.
        """
        chan = self.get_channel(channel)
        await chan.start()

    async def stop_channel(self, channel):
        """
        Stop the specified channel

        :params channel: The channel name to stop.
        """
        chan = self.get_channel(channel)
        await chan.start()

    async def list_msg(self, channel):
        """
        List first 10 messages from message store of specified channel.

        :params channel: The channel name.
        """
        # TODO allow indexing
        chan = self.get_channel(channel)
        result = list(itertools.islice(chan.message_store.search(), 10))
        for res in result:
            res['message'] = res['message'].to_json()
        return result

    async def replay_msg(self, channel, msg_ids):
        """
        Replay messages from message store.

        :params channel: The channel name.
        :params msg_ids: The message ids list to replay.
        """
        chan = self.get_channel(channel)
        result = []
        for msg_id in msg_ids:
            result.append((await chan.replay(msg_id)).to_dict())

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
    def __init__(self, url):
        self.url = url
        self.loop = asyncio.get_event_loop()

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
        async with websockets.connect(self.url) as ws:
            response = await WebSocketsClient(ws).request(command, *args)
            return response

    def exec(self, command):
        """
        Execute any valid python code on remote instance
        and return stout result.
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

    def list_msg(self, channel):
        """
        List first 10 messages on specified channel from remote instance.

        :params channel: The channel name.
        :returns: list of message with status.
        """
        result = self.send_command('list_msg', [channel])
        for res in result:
            res['message'] = message.Message.from_json(res['message'])
        return result

    def replay_msg(self, channel, msg_ids):
        """
        Replay specified message from id list of specified channel on remote instance.

        :params channel: The channel name.
        :params msg_ids: Message id list to replay
        :returns: List of result message.
        """
        result = self.send_command('replay_msg', [channel, msg_ids])
        return [message.Message.from_dict(msg) for msg in result]

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
        self.client = RemoteAdminClient(url)
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
        "List last 10 messages of selected channel"
        result = self.client.list_msg(channel)
        for res in result:
            print(res['message'].timestamp, res['id'], res['state'])

    @_with_current_channel
    def do_replay(self, channel, arg):
        "Replay a message list by their ids"
        msg_ids = arg.split()
        results = self.client.replay_msg(channel, msg_ids)
        for msg_id, msg in zip(msg_ids, results):
            print("Result message for replaying message %s:" % msg_id)
            print(msg.to_print())

    @_with_current_channel
    def do_push(self, channel, arg):
        "Inject message with text as payload for selected channel"
        result = self.client.push_msg(channel, arg)
        print("Result message:")
        print(result.to_print())

    def do_exit(self, arg):
        "Exit program"
        sys.exit()