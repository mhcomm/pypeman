import unittest
import asyncio
import logging


class FakeChannel():
    def __init__(self):
        self.logger = logging.getLogger()
        self.uuid = 'fakeChannel'

class NodesTests(unittest.TestCase):
   def setUp(self):
      self.loop = asyncio.new_event_loop()
      #asyncio.set_event_loop(None)

   def test_log_node(self):
        """ if Log() node is functionnal """
        from pypeman.nodes import Log
        from pypeman import message

        n = Log()
        n.channel = FakeChannel()

        m = message.Message()

        @asyncio.coroutine
        def go():
            ret = yield from n.handle(m)
            return ret

        self.loop.run_until_complete(go())

   def test_json_to_python_node(self):
       """ if JsonToPython() node is functionnal """
       from pypeman.nodes import JsonToPython
       from pypeman import message

       n = JsonToPython()
       n.channel = FakeChannel()

       m = message.Message()
       m.payload = '{"test":2}'

       @asyncio.coroutine
       def go():
           ret = yield from n.handle(m)
           return ret

       self.loop.run_until_complete(go())

