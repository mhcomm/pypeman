import unittest
import asyncio
import logging
from pypeman import nodes, message
import time

message_content = """{"test":1}"""

class FakeChannel():
    def __init__(self, loop):
        self.logger = logging.getLogger()
        self.uuid = 'fakeChannel'
        self.name = 'fakeChannel'
        self.parent_uids = "parent_uid"
        self.parent_names = ["parent_names"]

        self.loop = loop


class LongNode(nodes.ThreadNode):

    def process(self, msg):
        time.sleep(1)
        return msg

def generate_msg():
    # Default message
    m = message.Message()
    m.payload = message_content

    return m

class NodesTests(unittest.TestCase):
   def setUp(self):
       # Create class event loop used for tests to avoid failing
       # previous tests to impact next test ? (Not shure)
       self.loop = asyncio.new_event_loop()
       # Remove thread event loop to be sure we are not using
       # another event loop somewhere
       asyncio.set_event_loop(None)

   def test_log_node(self):
        """ if Log() node is functionnal """

        n = nodes.Log()
        n.channel = FakeChannel(self.loop)

        m = generate_msg()

        @asyncio.coroutine
        def go():
            ret = yield from n.handle(m)
            return ret

        self.loop.run_until_complete(go())

   def test_json_to_python_node(self):
       """ if JsonToPython() node is functionnal """

       n = nodes.JsonToPython()
       n.channel = FakeChannel(self.loop)

       m = generate_msg()

       @asyncio.coroutine
       def go():
           ret = yield from n.handle(m)
           return ret

       self.loop.run_until_complete(go())

   def test_thread_node(self):
       """ if Thread node is functionnal """

       # TODO test if another task can be executed in //

       n = LongNode()
       n.channel = FakeChannel(self.loop)

       m = generate_msg()

       @asyncio.coroutine
       def go():
           ret = yield from n.handle(m)
           return ret

       self.loop.run_until_complete(go())

