import json
import asyncio

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
