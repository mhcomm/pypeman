import unittest
from pypeman.message import Message
from .common import generate_msg
import logging
from unittest import mock


class MessageTests(unittest.TestCase):

    def test_message_dict_conversion(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        mdict = m.to_dict()

        self.assertTrue(isinstance(mdict, dict), "Message as_dict method is broken")

        compare_to = Message.from_dict(mdict)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test']['payload']['question'], compare_to.ctx['test']['payload']['question'], "Bad ctx")
        self.assertEqual(m.ctx['test']['meta']['answer'], compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_json_conversion(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        msg_json = m.to_json()

        compare_to = Message.from_json(msg_json)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(m.ctx['test']['payload']['question'], compare_to.ctx['test']['payload']['question'], "Bad ctx")
        self.assertEqual(m.ctx['test']['meta']['answer'], compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_copy(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        compare_to = m.copy()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertEqual(m.uuid.hex, compare_to.uuid.hex, "Bad uuid copy")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(m.ctx['test']['payload']['question'], compare_to.ctx['test']['payload']['question'], "Bad ctx copy")
        self.assertEqual(m.ctx['test']['meta']['answer'], compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_renew(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        compare_to = m.renew()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertNotEqual(m.uuid.hex, compare_to.uuid.hex, "Uuid should not be copied")
        self.assertNotEqual(m.timestamp, compare_to.timestamp, "Timestamp should not be copied")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(m.ctx['test']['payload']['question'], compare_to.ctx['test']['payload']['question'], "Bad ctx copy")
        self.assertEqual(m.ctx['test']['meta']['answer'], compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_logging(self):
        """
        Whether message logging is working well.
        """
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        mock_logger = mock.MagicMock()
        m.log(logger=mock_logger)

        mock_logger.log.assert_called_with(logging.DEBUG, 'Meta: %r', {'question': 'unknown'})
        mock_logger.reset()

        m.log(logger=mock_logger, payload=True, meta=True, context=True, log_level=logging.INFO)
        mock_logger.log.assert_called_with(logging.INFO, 'Meta: %r', {'answer': 43})
