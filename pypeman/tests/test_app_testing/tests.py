from pypeman.test import PypeTestCase
from pypeman import message


class Tests(PypeTestCase):

    def test_simple(self):
        msg = message.Message()
        msg.payload = "test"

        chan = self.get_channel('base')

        end = chan.handle_and_wait(msg)

        self.assertEqual(end.payload, 'test')
