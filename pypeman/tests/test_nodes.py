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


class NodesTests(unittest.TestCase):
    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

    def test_log_node(self):
        """ if Log() node functional """

        n = nodes.Log()
        n.channel = FakeChannel(self.loop)

        m = generate_msg()

        @asyncio.coroutine
        def go():
            ret = yield from n.handle(m)
            # Check return
            self.assertTrue(isinstance(ret, message.Message))
            return ret

        self.loop.run_until_complete(go())


    def test_base_node(self):
        """ if BaseNode() node functional """

        n = nodes.BaseNode()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        # TODO Simplify test
        @asyncio.coroutine
        def go():
           ret = yield from n.handle(m)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))
           self.assertEqual(ret.payload, 'test', "Base node not working !")
           self.assertEqual(n.processed, 1, "Processed msg count broken")
           return ret

        self.loop.run_until_complete(go())


    def test_sleep_node(self):
        """ if Sleep() node functional """

        n = nodes.Sleep()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        @asyncio.coroutine
        def go():
           ret = yield from n.handle(m)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))
           self.assertEqual(ret.payload, 'test', "Sleep node not working !")
           return ret

        self.loop.run_until_complete(go())

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

        @asyncio.coroutine
        def go():
           ret = yield from n1.handle(m)
           ext_new = yield from n2.handle(ret)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))
           self.assertEqual(base, ext_new.payload, "B64 nodes not working !")

        self.loop.run_until_complete(go())

    def test_json_to_python_node(self):
        """ if JsonToPython() node functional """

        n = nodes.JsonToPython()
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='{"test":1}')

        @asyncio.coroutine
        def go():
           ret = yield from n.handle(m)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))
           self.assertEqual(ret.payload, {"test":1}, "JsonToPython node not working !")

           return ret

        self.loop.run_until_complete(go())

    def test_thread_node(self):
        """ if Thread node functional """

        # TODO test if another task can be executed in //

        n = LongNode()
        n.channel = FakeChannel(self.loop)

        m = generate_msg()

        @asyncio.coroutine
        def go():
           ret = yield from n.handle(m)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))

           return ret

        self.loop.run_until_complete(go())

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

        @asyncio.coroutine
        def go():
           ret = yield from n.handle(m)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))

           return ret

        self.loop.run_until_complete(go())

    def test_save_node(self):
        """ if Save() node functional """

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file, \
                mock.patch('pypeman.nodes.os.makedirs') as mock_makedirs:
            mock_makedirs.return_value = None

            n = nodes.Save(uri='file:///tmp/test/?filename=%(msg_year)s/%(msg_month)s/message%(msg_day)s-%(counter)s.txt')
            n.channel = FakeChannel(self.loop)

            m = generate_msg(timestamp=(1981, 12, 28, 13, 37))
            m.payload = "content"

            @asyncio.coroutine
            def go():
                ret = yield from n.handle(m)
                # Check return
                self.assertTrue(isinstance(ret, message.Message))

                return ret

            self.loop.run_until_complete(go())

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

        @asyncio.coroutine
        def go():
           ret = yield from n1.handle(m)
           ext_new = yield from n2.handle(ret)
           # Check return
           self.assertTrue(isinstance(ret, message.Message))
           self.assertEqual(base, ext_new.payload, "XML nodes not working !")

        self.loop.run_until_complete(go())

