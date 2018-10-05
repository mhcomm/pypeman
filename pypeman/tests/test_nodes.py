import asyncio
import os
import time
import unittest

from unittest import mock

import aiohttp
from pypeman import nodes, message, conf, persistence

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


def get_mock_coro(return_value):
    @asyncio.coroutine
    def mock_coro(*args, **kwargs):
        return return_value

    return mock.Mock(wraps=mock_coro)


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

    def test_node_persistence(self):
        """ Whether BaseNode() data memory persistence works"""
        persistence._backend = None # noqa

        # Mock settings
        with mock.patch('pypeman.persistence.settings',
                        conf.Settings(module_name='pypeman.tests.settings.test_settings_persistence')):
            result = []

            class TestPers(nodes.BaseNode):
                """ Test node for persistence """

                async def process(self, msg):
                    await self.save_data('test', 'value')
                    result.append(await self.restore_data('test'))
                    result.append(await self.restore_data('titi', default='Yo'))
                    try:
                        result.append(await self.restore_data('titi'))
                    except KeyError:
                        result.append('yay')

                    return msg

            n = TestPers()

            n.channel = FakeChannel(self.loop)

            m = generate_msg(message_content='test')

            self.loop.run_until_complete(n.handle(m))

            self.assertEqual(result[0], 'value', "Can't persist data for node")
            self.assertEqual(result[1], 'Yo', "Default value not working")
            self.assertEqual(result[2], 'yay', "Exception on missing key not working")

    def test_node_sqlite_persistence(self):
        """ Whether BaseNode() data sqlite persistence works"""
        persistence._backend = None # noqa

        db_path = '/tmp/to_be_removed_849827198746.sqlite'

        # Mock settings
        with mock.patch('pypeman.persistence.settings',
                        conf.Settings(module_name='pypeman.tests.settings.test_settings_sqlite_persist')):
            result = []

            class TestPers(nodes.BaseNode):
                """ Test node for persistence """

                async def process(self, msg):
                    await self.save_data('test', 'value')
                    result.append(await self.restore_data('test'))
                    result.append(await self.restore_data('titi', default='Yo'))
                    try:
                        result.append(await self.restore_data('titi'))
                    except KeyError:
                        result.append('yay')

                    return msg

            n = TestPers()

            n.channel = FakeChannel(self.loop)

            m = generate_msg(message_content='test')
            print('self', id(self.loop))

            self.loop.run_until_complete(n.handle(m))

            self.assertEqual(result[0], 'value', "Can't persist data for node")
            self.assertEqual(result[1], 'Yo', "Default value not working")
            self.assertEqual(result[2], 'yay', "Exception on missing key not working")

            os.remove(db_path)

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

    def test_drop_node(self):
        """ Whether Drop() node is working """

        msg_to_show = "It's only dropped"
        n = nodes.Drop(message=msg_to_show)
        n.channel = FakeChannel(self.loop)

        m = generate_msg(message_content='test')

        with self.assertRaises(nodes.Dropped) as cm:
            self.loop.run_until_complete(n.handle(m))

        self.assertEqual(str(cm.exception), msg_to_show, "Drop node message not working !")

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
        self.assertEqual(ret.payload, {"test": 1}, "JsonToPython node not working !")

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

        n = nodes.Email(host="mailtrap.io", port=2525, start_tls=False,
                        ssl=False, user=smtp_user, password=smtp_password,
                        subject="Sent from email node of Pypeman",
                        sender=recipients, recipients=recipients)
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

            n = nodes.Save(uri='file:///tmp/test/?filename=%(msg_year)s/'
                           '%(msg_month)s/message%(msg_day)s-%(counter)s.txt')
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
            import xmltodict  # noqa F401
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

        with mock.patch('pypeman.contrib.ftp.FTPHelper', new=fake_ftp_helper):

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

    def test_httprequest_node(self):
        """ Whether HttpRequest node is functional """

        channel = FakeChannel(self.loop)

        auth = ("login", "mdp")
        url = 'http://url/%(meta.beta)s/%(payload.alpha)s'
        b_auth = aiohttp.BasicAuth(auth[0], auth[1])
        client_cert = ('/cert.key', '/cert.crt')
        http_node1 = nodes.HttpRequest(url=url, verify=False, auth=auth)
        http_node1.channel = channel

        content1 = {"alpha": "payload_url"}
        msg1 = generate_msg(message_content=content1)
        meta_params = {'omega': 'meta_params'}
        headers1 = {'test': 'test'}
        msg1.meta = {"beta": "meta_url", 'params': meta_params, 'headers': headers1}
        req_url1 = 'http://url/meta_url/payload_url'
        req_kwargs1 = {
            'data': None,
            'params': [('omega', 'meta_params')],
            'url': req_url1,
            'headers': headers1,
            'method': 'get',
            'auth': b_auth
            }

        msg2 = generate_msg(message_content=content1)
        msg2.meta = dict(msg1.meta)
        msg2.meta['method'] = 'post'
        msg2.meta['params'] = {'zeta': ['un', 'deux', 'trois']}
        req_kwargs2 = dict(req_kwargs1)
        req_kwargs2['method'] = 'post'
        req_kwargs2['params'] = [
                ('zeta', 'un'),
                ('zeta', 'deux'),
                ('zeta', 'trois'),
                # ('omega', 'meta_params')
                ]
        req_kwargs2['data'] = content1

        args_headers = {'args_headers': 'args_headers'}
        args_params = {'theta': ['uno', 'dos']}
        http_node2 = nodes.HttpRequest(
            url=url,
            method='post',
            client_cert=client_cert,
            auth=b_auth,
            headers=args_headers,
            params=args_params
        )
        http_node2.channel = channel
        msg3 = msg1.copy()
        req_kwargs3 = dict(req_kwargs1)
        req_kwargs3['method'] = 'post'
        req_kwargs3['params'] = [
                ('theta', 'uno'),
                ('theta', 'dos'),
                ]
        req_kwargs3['headers'] = args_headers
        req_kwargs3['data'] = content1

        with mock.patch(
            'pypeman.contrib.http.aiohttp.ClientSession',
            autospec=True) as mock_client_session, mock.patch(
                'ssl.SSLContext',
                autospec=True) as mock_ssl_context:
            mock_ctx_mgr = mock_client_session.return_value
            mock_session = mock_ctx_mgr.__enter__.return_value
            mg = mock.MagicMock()
            mg.text = get_mock_coro(mock.MagicMock())
            mock_session.request = get_mock_coro(mg)
            mock_load_cert_chain = mock_ssl_context.return_value.load_cert_chain

            """
                Test 1:
                - default get,
                - auth tuple in object BasicAuth,
                - simple dict params from meta,
                - headers from meta
                - url construction
            """
            self.loop.run_until_complete(http_node1.handle(msg1))
            mock_session.request.assert_called_once_with(**req_kwargs1)
            mock_load_cert_chain.assert_not_called()

            mock_session.reset_mock()

            """
                Test 2:
                - post in meta with data from content,
                - list in dict params from meta,
            """
            self.loop.run_until_complete(http_node1.handle(msg2))
            mock_session.request.assert_called_once_with(**req_kwargs2)
            mock_load_cert_chain.assert_not_called()

            mock_session.reset_mock()

            """
                Test 3:
                - post in node args,
                - object BasicAuth for auth,
                - list in dict params from args,
                - headers from args
                - client_cert
            """
            self.loop.run_until_complete(http_node2.handle(msg3))
            mock_session.request.assert_called_once_with(**req_kwargs3)
            mock_load_cert_chain.assert_called_once_with(client_cert[0], client_cert[1])

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
            self.loop.run_until_complete(writer.handle(msg1))

        mock_file.assert_called_once_with('/filepath', 'w')
        handle = mock_file()
        handle.write.assert_called_once_with('message_content')

        writer2 = nodes.FileWriter(safe_file=False)
        writer.channel = channel
        msg2 = generate_msg(message_content="message_content2")
        msg2.meta['filepath'] = '/filepath2'
        with mock.patch("builtins.open", mock.mock_open()) as mock_file:
            self.loop.run_until_complete(writer2.handle(msg2))

        mock_file.assert_called_once_with('/filepath2', 'w')
        handle = mock_file()
        handle.write.assert_called_once_with('message_content2')
