import unittest
from pypeman.message import Message


def create_message():
    m = Message()

    m.payload = {'answer': 42}
    m.meta['question'] = 'unknown'

    mctx = Message()

    mctx.meta['answer'] = 42
    mctx.payload = {'question': 'unknown'}

    m.ctx['test'] = mctx

    return m


class MessageTests(unittest.TestCase):

    def test_message_dict_conversion(self):


        m = create_message()

        mdict = m.as_dict()

        self.assertTrue(isinstance(mdict, dict), "Message as_dict method is broken")

        compare_to = Message.from_dict(mdict)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx")

    def test_message_json_conversion(self):
        m = create_message()

        msg_json = m.to_json()

        compare_to = Message.from_json(msg_json)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx")


    def test_message_copy(self):
        m = create_message()

        compare_to = m.copy()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid copy")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx copy")









