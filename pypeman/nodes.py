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
        print(msg)
        return msg


class JsonToPython(Node):
    def process(self, msg):
        result = json.loads(msg)
        return result


class PythonToJson(Node):
    def process(self, msg):
        result = json.dumps(msg)
        return result


class Add1(Node):
    def process(self, msg):
        msg['sample'] += 1
        return msg
