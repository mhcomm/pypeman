import json
import asyncio

class Node:
    @asyncio.coroutine
    def handle(self, msg):
        return self.process(msg)

    def process(self, msg):
        return msg


class Log(Node):
    def process(self, msg):
        print(msg.payload)
        return msg


class JsonToPython(Node):
    def process(self, msg):
        msg.payload = json.loads(msg.payload)
        msg.content_type = 'application/python'
        return msg


class PythonToJson(Node):
    def process(self, msg):
        msg.payload = json.dumps(msg.payload)
        msg.content_type = 'application/json'
        return msg


class Add1(Node):
    def process(self, msg):
        msg.payload['sample'] += 1
        return msg
