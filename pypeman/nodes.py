import json
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

loop = asyncio.get_event_loop()


class BaseNode:
    @asyncio.coroutine
    def handle(self, msg):
        return self.process(msg)

    def process(self, msg):
        return msg


class Log(BaseNode):
    def process(self, msg):
        print(msg.payload)
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


class Add1(BaseNode):
    def process(self, msg):
        msg.payload['sample'] += 1
        return msg


class ThreadNode(BaseNode):

    @asyncio.coroutine
    def handle(self, msg):
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = yield from loop.run_in_executor(executor, self.process, msg)
            return result
