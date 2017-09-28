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

from pypeman.conf import settings
from pypeman import channels
from pypeman import message

logger = logging.getLogger(__name__)

class RemoteServer():

    async def start(self):
        """ Start remote admin server """
        start_server = websockets.serve(
            self.command,
            settings.REMOTE_ADMIN_HOST,
            settings.REMOTE_ADMIN_PORT
        )
        await start_server

    def get_channel(self, name):
        for chan in channels.all:
            if chan.name == name:
                return chan
        return None

    async def command(self, websocket, path):
        try:
            while True:
                json_cmd = await websocket.recv()
                cmd = json.loads(json_cmd)
                logger.debug("Receive command $ %s", cmd)
                command = cmd['command']
                args = cmd['args']

                if command == "channels":
                    chans = []
                    for chan in channels.all:
                        if not chan.parent:
                            chans.append({
                                'name': chan.name,
                                'status': channels.BaseChannel.status_id_to_str(chan.status)
                            })
                    response = {'status':'ok', 'content':chans}

                elif command == "stop":
                    for c in args:
                        chan = self.get_channel(c)
                        await chan.stop()
                    response = {'status':'ok', 'content': "Channel(s) stopped"}

                elif command == "start":
                    for c in args:
                        chan = self.get_channel(c)
                        await chan.start()
                    response = {'status':'ok', 'content': "Channel(s) started"}

                elif command == "listmsg":
                    chan = self.get_channel(args[0])
                    result = list(itertools.islice(chan.message_store.search(), 10))
                    response = {'status':'ok', 'content': result}

                elif command == "pushtextmsg":
                    chan = self.get_channel(args[0])
                    msg = message.Message(payload=args[1])
                    result = await chan.handle(msg)
                    response = {'status':'ok', 'content': result.to_dict()}

                else:
                    response = {'status':"error", 'content':"Command not recognized!"}

                json_response = json.dumps(response)
                await websocket.send(json_response)

        except websockets.exceptions.ConnectionClosed:
            logger.debug('Client disconnected')

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
    intro = 'Welcome to the pypeman shell.   Type help or ? to list commands.\n'
    prompt = 'pypeman > '
    use_rawinput = False

    def __init__(self):
        super().__init__()
        self.current_channel = None
        self.loop = asyncio.get_event_loop()

    def send_command(self, cmd):
        try:
            return self.loop.run_until_complete(self._send_command(cmd))
        except Exception as e: # noqa
            logger.exception("Error while sending message")
            print("Are you sure pypeman is working?")
            return {'status': 'error', 'content': e}

    async def _send_command(self, cmd):
        async with websockets.connect('ws://%s:%s' % (settings.REMOTE_ADMIN_HOST,
                                                    settings.REMOTE_ADMIN_PORT)) as websocket:
            json_cmd = json.dumps(cmd)
            await websocket.send(json_cmd)
            json_response = await websocket.recv()
            return json.loads(json_response)

    def do_channels(self, arg):
        "List avaible channels"
        cmd = {
            'command': 'channels',
            'args': [arg]
        }
        response = self.send_command(cmd)
        for c in response['content']:
            print("{c[name]} - {c[status]}".format(c=c))

        self.current_channel = None
        self.prompt = "pypeman > " % arg

    def do_stop(self, arg):
        "Stop a channel by his name"
        cmd = {
            'command': 'stop',
            'args': arg.split()
        }
        response = self.send_command(cmd)
        print(response)

    def do_start(self, arg):
        "Stop a channel by his name"
        cmd = {
            'command': 'start',
            'args': arg.split()
        }
        response = self.send_command(cmd)
        print(response)

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
        cmd = {
            'command': 'listmsg',
            'args': [channel]
        }
        response = self.send_command(cmd)
        print(response)

    @_with_current_channel
    def do_push(self, channel, arg):
        "Inject message from text for selected channel"
        cmd = {
            'command': 'pushtextmsg',
            'args': [channel, arg]
        }
        print(cmd)
        response = self.send_command(cmd)
        print(response)

    def do_exit(self, arg):
        "Exit program"
        sys.exit(0)

