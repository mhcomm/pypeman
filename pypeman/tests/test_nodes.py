import os
import unittest
import asyncio
import logging
import time
from unittest import mock

from pypeman import nodes, message

from pypeman.tests.common import generate_msg


class FakeChannel():
    def __init__(self, loop):
        self.logger = mock.MagicMock()
        self.uuid = 'fakeChannel'
        self.name = 'fakeChannel'
        self.parent_uids = "parent_uid"
        self.parent_names = ["parent_names"]

        self.loop = loop

class LongNode(nodes.ThreadNode):
    def process(self, msg):
        time.sleep(1)
        return msg

def tstfct(msg):
    return '/fctpath'

def tstfct2(msg):
    return 'fctname'

class NodesTests(unittest.TestCase):
    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

    def test_base_node(self):
        """ if BaseNode() node functional """

        n = nodes.BaseNode()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        ret = self.loop.run_until_complete(n.handle(m))

        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(ret.payload, 'test', "Base node not working !")
        self.assertEqual(n.processed, 1, "Processed msg count broken")

    def test_base_logging(self):
        """ whether BaseNode() node logging works"""

        n = nodes.BaseNode(log_output=True)
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        ret = self.loop.run_until_complete(n.handle(m))

        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(ret.payload, 'test', "Base node not working !")
        self.assertEqual(n.processed, 1, "Processed msg count broken")

        n.channel.logger.log.assert_any_call(10, 'Payload: %r', 'test')
        n.channel.logger.log.assert_called_with(10, 'Meta: %r', {'question': 'unknown'})


    def test_log_node(self):
        """ whether Log() node functional """

        n = nodes.Log()
        n.channel = FakeChannel(self.loop)

        m = generate_msg()

        ret = self.loop.run_until_complete(n.handle(m))

        self.assertTrue(isinstance(ret, message.Message))

    def test_sleep_node(self):
        """ if Sleep() node functional """

        n = nodes.Sleep()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        ret = self.loop.run_until_complete(n.handle(m))

        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(ret.payload, 'test', "Sleep node not working !")

    def test_b64_nodes(self):
        """ if B64 nodes are functional """

        n1 = nodes.B64Encode()
        n2 = nodes.B64Decode()

        channel = FakeChannel(self.loop)

        n1.channel = channel
        n2.channel = channel

        m = generate_msg()

        m.payload = b'hello'

        base = bytes(m.payload)

        ret = self.loop.run_until_complete(n1.handle(m))
        ext_new = self.loop.run_until_complete(n2.handle(ret))

        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(base, ext_new.payload, "B64 nodes not working !")

    def test_json_to_python_node(self):
        """ if JsonToPython() node functional """

        n = nodes.JsonToPython()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='{"test":1}')

        ret = self.loop.run_until_complete(n.handle(m))
        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(ret.payload, {"test":1}, "JsonToPython node not working !")

    def test_thread_node(self):
        """ if Thread node functional """

        # TODO test if another task can be executed in //

        n = LongNode()
        n.channel = FakeChannel(self.loop)

        m = generate_msg()

        ret = self.loop.run_until_complete(n.handle(m))
        # Check return
        self.assertTrue(isinstance(ret, message.Message))

    @unittest.skipIf(not os.environ.get('PYPEMAN_TEST_SMTP_USER')
                     or not os.environ.get('PYPEMAN_TEST_SMTP_PASSWORD')
                     or not os.environ.get('PYPEMAN_TEST_RECIPIENT_EMAIL'),
                     "Email node test skipped. Set PYPEMAN_TEST_SMTP_USER, PYPEMAN_TEST_SMTP_PASSWORD and "
                        "PYPEMAN_TEST_RECIPIENT_EMAIL to enable it.")
    def test_email_node(self):
        """ if Email() node is functional """

        # Set this three vars to enable the skipped test.
        # Use mailtrap free account to test it.
        recipients = os.environ['PYPEMAN_TEST_RECIPIENT_EMAIL']
        smtp_user = os.environ['PYPEMAN_TEST_SMTP_USER']
        smtp_password = os.environ['PYPEMAN_TEST_SMTP_PASSWORD']

        n = nodes.Email(host="mailtrap.io", port=2525, start_tls=False, ssl=False, user=smtp_user, password=smtp_password,
                        subject="Sent from email node of Pypeman", sender=recipients, recipients=recipients)
        n.channel = FakeChannel(self.loop)

        m = generate_msg()
        m.payload = "Message content is full of silence !"

        ret = self.loop.run_until_complete(n.handle(m))

        self.assertTrue(isinstance(ret, message.Message))

    def test_save_node(self):
        """ if Save() node functional """

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file, \
                mock.patch('pypeman.nodes.os.makedirs') as mock_makedirs:

            mock_makedirs.return_value = None

            n = nodes.Save(uri='file:///tmp/test/?filename=%(msg_year)s/%(msg_month)s/message%(msg_day)s-%(counter)s.txt')
            n.channel = FakeChannel(self.loop)

            m = generate_msg(timestamp=(1981, 12, 28, 13, 37))
            m.payload = "content"

            ret = self.loop.run_until_complete(n.handle(m))

            self.assertTrue(isinstance(ret, message.Message))

            # Asserts
            mock_makedirs.assert_called_once_with('/tmp/test/1981/12')
            mock_file.assert_called_once_with('/tmp/test/1981/12/message28-0.txt', 'w')
            handle = mock_file()
            handle.write.assert_called_once_with('content')


    def test_xml_nodes(self):
        """ if XML nodes are functional """
        try:
            import xmltodict
        except ImportError:
            raise unittest.SkipTest("Missing dependency xmltodict.")

        n1 = nodes.XMLToPython()
        n2 = nodes.PythonToXML()

        channel = FakeChannel(self.loop)

        n1.channel = channel
        n2.channel = channel

        m = generate_msg()

        m.payload = '<?xml version="1.0" encoding="utf-8"?>\n<test>hello</test>'

        base = str(m.payload)

        ret = self.loop.run_until_complete(n1.handle(m))
        ext_new = self.loop.run_until_complete(n2.handle(ret))
        # Check return
        self.assertTrue(isinstance(ret, message.Message))
        self.assertEqual(base, ext_new.payload, "XML nodes not working !")

    def test_ftp_nodes(self):
        """ Whether FTP nodes are functional """

        channel = FakeChannel(self.loop)

        ftp_config = dict(host="fake", credentials=("fake", "fake"))

        fake_ftp = mock.MagicMock()
        fake_ftp.download_file = mock.Mock(return_value=b"new_content")

        fake_ftp_helper = mock.Mock(return_value=fake_ftp)

        with mock.patch('pypeman.contrib.ftp.FTPHelper', new=fake_ftp_helper) as mock_ftp:

            reader = nodes.FTPFileReader(filepath="test_read", **ftp_config)
            delete = nodes.FTPFileDeleter(filepath="test_delete", **ftp_config)

            writer = nodes.FTPFileWriter(filepath="test_write", **ftp_config)

            reader.channel = channel
            delete.channel = channel
            writer.channel = channel

            m1 = generate_msg(message_content="to_be_replaced")
            m1_delete = generate_msg(message_content="to_be_replaced")
            m2 = generate_msg(message_content="message_content")

            # Test reader
            result = self.loop.run_until_complete(reader.handle(m1))

            fake_ftp.download_file.assert_called_once_with('test_read')
            self.assertEqual(result.payload, b"new_content", "FTP reader not working")

            # Test reader with delete after
            result = self.loop.run_until_complete(delete.handle(m1_delete))

            fake_ftp.delete.assert_called_once_with('test_delete')

            # test writer
            result = self.loop.run_until_complete(writer.handle(m2))
            fake_ftp.upload_file.assert_called_once_with('test_write.part', 'message_content')
            fake_ftp.rename.assert_called_once_with('test_write.part', 'test_write')


    def test_file_reader_node(self):
        """if FileReader are functionnal"""

        reader = nodes.FileReader(filepath='/filepath', filename='badname')
        channel = FakeChannel(self.loop)

        reader.channel = channel
        msg1 = generate_msg()

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            result = self.loop.run_until_complete(reader.handle(msg1))

        mock_file.assert_called_once_with('/filepath', 'r')
        self.assertEqual(result.payload, "data", "FileReader not working")

        reader2 = nodes.FileReader()
        reader2.channel = channel
        msg2 = generate_msg()
        msg2.meta['filepath'] = '/filepath2'
        msg2.meta['filename'] = '/badpath'

        with mock.patch("builtins.open", mock.mock_open(read_data="data2")) as mock_file:
            result = self.loop.run_until_complete(reader2.handle(msg2))

        mock_file.assert_called_once_with('/filepath2', 'r')
        self.assertEqual(result.payload, "data2", "FileReader not working with meta")


        reader3 = nodes.FileReader(filepath=tstfct, filename='badname')
        reader3.channel = channel

        msg3 = generate_msg()
        msg3.meta['filepath'] = '/badpath'
        msg3.meta['filename'] = 'badname2'

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            result = self.loop.run_until_complete(reader3.handle(msg3))

        mock_file.assert_called_once_with('/fctpath', 'r')

        reader4 = nodes.FileReader(filename=tstfct2)
        reader4.channel = channel
        msg4 = generate_msg()
        msg4.meta['filepath'] = '/filepath3/badname'
        msg4.meta['filename'] = 'badname'

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            result = self.loop.run_until_complete(reader4.handle(msg4))

        mock_file.assert_called_once_with('/filepath3/fctname', 'r')

    def test_file_writer_node(self):
        """Whether FileWriter is functionnal"""
        writer = nodes.FileWriter(filepath='/filepath', safe_file=False)
        channel = FakeChannel(self.loop)
        writer.channel = channel
        msg1 = generate_msg(message_content="message_content")
        with mock.patch("builtins.open", mock.mock_open()) as mock_file:
            result = self.loop.run_until_complete(writer.handle(msg1))

        mock_file.assert_called_once_with('/filepath', 'w')
        handle = mock_file()
        handle.write.assert_called_once_with('message_content')

        writer2 = nodes.FileWriter(safe_file=False)
        writer.channel = channel
        msg2 = generate_msg(message_content="message_content2")
        msg2.meta['filepath'] = '/filepath2'
        with mock.patch("builtins.open", mock.mock_open()) as mock_file:
            result = self.loop.run_until_complete(writer2.handle(msg2))

        mock_file.assert_called_once_with('/filepath2', 'w')
        handle = mock_file()
        handle.write.assert_called_once_with('message_content2')
