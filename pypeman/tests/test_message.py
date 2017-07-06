import unittest
from pypeman.message import Message
from .common import generate_msg
import logging
from unittest import mock


def create_complex_message():
    m = generate_msg(message_content={'answer': 42})

    mctx = generate_msg(message_content={'question': 'known'}, message_meta={'answer': 43})

    m.add_context('test', mctx)

    return m


class MessageTests(unittest.TestCase):

    def test_message_dict_conversion(self):


        m = create_complex_message()

        mdict = m.to_dict()

        self.assertTrue(isinstance(mdict, dict), "Message as_dict method is broken")

        compare_to = Message.from_dict(mdict)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx")

    def test_message_json_conversion(self):
        m = create_complex_message()

        msg_json = m.to_json()

        compare_to = Message.from_json(msg_json)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx")


    def test_message_copy(self):
        m = create_complex_message()

        compare_to = m.copy()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid copy")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx copy")

    def test_message_renew(self):
        m = create_complex_message()

        compare_to = m.renew()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertNotEqual(m.uuid.hex, compare_to.uuid.hex, "Uuid should not be copied")
        self.assertNotEqual(m.timestamp, compare_to.timestamp, "Timestamp should not be copied")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(m.ctx['test'].payload['question'], compare_to.ctx['test'].payload['question'], "Bad ctx copy")

    def test_message_logging(self):
        """
        Whether message logging is working well.
        """
        m = create_complex_message()

        mock_logger = mock.MagicMock()
        m.log(logger=mock_logger)

        mock_logger.log.assert_called_with(logging.DEBUG, 'Meta: %r', {'question': 'unknown'})
        mock_logger.reset()

        m.log(logger=mock_logger, payload=True, meta=True, context=True, log_level=logging.INFO)
        mock_logger.log.assert_called_with(logging.INFO, 'Meta: %r', {'answer': 43})
