import unittest
import asyncio


class FakeChannel():
    def __init__(self):
        self.uuid = 'fakeChannel'

class NodesTests(unittest.TestCase):
   def setUp(self):
      self.loop = asyncio.new_event_loop()
      #asyncio.set_event_loop(None)

   def test_log_node(self):
        """ Log() node is functionnal """
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
