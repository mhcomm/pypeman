import json
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from pypeman.message import Message
from pypeman.channels import Dropped, Break

loop = asyncio.get_event_loop()



class BaseNode:
    """ Base of all Node
    """
    def __init__(self, *args, **kwargs):
        self.blocking = kwargs.pop('blocking', True)
        self.immediate_ack = kwargs.pop('immediate_ack', False)
        self.channel = None

    @asyncio.coroutine
    def handle(self, msg):
        if not self.blocking:
            asyncio.async(asyncio.coroutine(self.process)(msg.copy()))
            result = msg
        else:
            result = yield from asyncio.coroutine(self.process)(msg)

        return result

    def process(self, msg):
        return msg


class RaiseError(BaseNode):
    def process(self, msg):
        raise Exception("Test node")


class DropNode(BaseNode):
    def process(self, msg):
        raise Dropped()


class BreakNode(BaseNode):
    def process(self, msg):
        raise Break()


class Log(BaseNode):
    def process(self, msg):
        print(self.channel.uuid, msg.payload)
        return msg


class JsonToPython(BaseNode):
    def process(self, msg):
        msg.payload = json.loads(msg.payload)
        msg.content_type = 'application/python'
        return msg


class PythonToJson(BaseNode):
    def process(self, msg):
        msg.payload = json.dumps(msg.payload)
        msg.content_type = 'application/json'
        return msg

class Empty(BaseNode):
    def process(self, msg):
        return Message()

class Add1(BaseNode):
    def process(self, msg):
        msg.payload['sample'] += 1
        return msg

'''class JoinNode(BaseNode):

    def add_input(self):
        pass

    def process(self, msg):
        # TODO wait for others inputs
        return msg'''

class ThreadNode(BaseNode):
    # Todo create class ThreadPool

    @asyncio.coroutine
    def handle(self, msg):
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = yield from loop.run_in_executor(executor, self.process, msg)
            return result
