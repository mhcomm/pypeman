import logging

from unittest import mock

from .common import generate_msg

from pypeman.message import Message
from pypeman.test import TearDownProjectTestCase as TestCase


class MessageTests(TestCase):

    def test_message_dict_conversion(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        mdict = m.to_dict()

        self.assertTrue(isinstance(mdict, dict), "Message as_dict method is broken")

        compare_to = Message.from_dict(mdict)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid, compare_to.uuid, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(
            m.ctx['test']['payload']['question'],
            compare_to.ctx['test']['payload']['question'], "Bad ctx")
        self.assertEqual(
            m.ctx['test']['meta']['answer'],
            compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_json_conversion(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        msg_json = m.to_json()

        compare_to = Message.from_json(msg_json)

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well decoded")
        self.assertEqual(m.uuid, compare_to.uuid, "Bad uuid")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta")

        self.assertEqual(
            m.ctx['test']['payload']['question'],
            compare_to.ctx['test']['payload']['question'], "Bad ctx")
        self.assertEqual(
            m.ctx['test']['meta']['answer'],
            compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_copy(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        compare_to = m.copy()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertEqual(m.uuid, compare_to.uuid, "Bad uuid copy")
        self.assertEqual(m.meta['question'], compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(
            m.ctx['test']['payload']['question'],
            compare_to.ctx['test']['payload']['question'], "Bad ctx copy")
        self.assertEqual(
            m.ctx['test']['meta']['answer'],
            compare_to.ctx['test']['meta']['answer'], "Bad ctx")

    def test_message_renew(self):
        m = generate_msg(message_content={'answer': 42}, with_context=True)

        compare_to = m.renew()

        self.assertEqual(m.payload['answer'], compare_to.payload['answer'], "Payload not well copied")
        self.assertNotEqual(m.uuid, compare_to.uuid, "Uuid should not be copied")
        self.assertNotEqual(m.timestamp, compare_to.timestamp, "Timestamp should not be copied")
        self.assertEqual(m.meta['question'],
                         compare_to.meta['question'], "Bad meta copy")

        self.assertEqual(
            m.ctx['test']['payload']['question'],
            compare_to.ctx['test']['payload']['question'], "Bad ctx copy")
        self.assertEqual(m.ctx['test']['meta']['answer'],
                         compare_to.ctx['test']['meta']['answer'], "Bad ctx")

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

    def test_message_print(self):
        """
        Whether message print version is working well.
        """
        m = generate_msg(
            message_content={'answer': 42},
            with_context=True,
            timestamp=(2010, 10, 10, 10, 10, 10),
        )
        m.uuid = "msguuid"

        result = m.to_print(context=True)

        reference = """Message msguuid
Date: 2010-10-10 10:10:10
Payload: {'answer': 42}
Meta: {'question': 'unknown'}
Context for message ->
-- Key "test" --
Payload: {'question': 'known'}
Meta: {'answer': 43}
"""

        self.assertEqual(result, reference, "Print message not working")
