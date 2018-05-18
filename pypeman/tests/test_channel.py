import os
import unittest
import asyncio
import datetime
import shutil
import tempfile
from socket import SOL_SOCKET
from unittest import mock

from pypeman import channels, endpoints
from pypeman.channels import BaseChannel
from pypeman import message
from pypeman import nodes
from pypeman import msgstore
from pypeman import events
from pypeman.errors import PypemanParamError
from pypeman.tests.common import TestException, generate_msg

class TestNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used to test if node is processed during test

    def process(self, msg):
        print("Process %s" % self.name)
        return msg


class TestConditionalErrorNode(nodes.BaseNode):

    def process(self, msg):
        print("Process %s" % self.name)

        if msg.timestamp.day == 12:
            raise TestException()

        return msg


class ExceptNode(TestNode):
    # This node raises an exception
    def process(self, msg):
        result = super().process(msg)
        raise TestException()


class ChannelsTests(unittest.TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks
        pending = asyncio.Task.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending))

    def start_channels(self):
        # Start channels
        for chan in channels.all:
            self.loop.run_until_complete(chan.start())

    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

        # Avoid calling already tested channels
        channels.all.clear()

    def tearDown(self):
        self.clean_loop()

    def test_base_channel(self):
        """ Whether BaseChannel handling is working """

        chan = BaseChannel(name="test_channel1", loop=self.loop)
        n = TestNode()
        msg = generate_msg()

        same_chan = chan.add(n)

        self.assertEqual(same_chan, chan, "Add doesn't return channel")

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_no_node_base_channel(self):
        """ Whether BaseChannel handling is working even if there is no node """

        chan = BaseChannel(name="test_channel2", loop=self.loop)
        msg = generate_msg()

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

    def test_sub_channel(self):
        """ Whether Sub Channel is working """

        chan = BaseChannel(name="test_channel3", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = TestNode(name="sub1")
        n4 = TestNode(name="sub2")

        msg = generate_msg()

        same_chan = chan.append(n1)

        self.assertEqual(chan, same_chan, "Append don't return channel.")

        sub = chan.fork(name="subchannel")
        sub.append(n2, n3, n4)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")
        self.assertTrue(n3.processed, "Sub Channel not working")
        self.assertTrue(n4.processed, "Sub Channel not working")
        self.assertEqual(sub.name, "test_channel3.subchannel", "Subchannel name is incorrect")

    def test_sub_channel_with_exception(self):
        """ Whether Sub Channel exception handling is working """

        chan = BaseChannel(name="test_channel4", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="sub")
        n3 = ExceptNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork(name="Hello")
        sub.add(n2, n3)

        # Launch channel processing
        self.start_channels()

        self.loop.run_until_complete(chan.handle(msg))

        self.assertEqual(n1.processed, 1, "Sub Channel not working")

        with self.assertRaises(TestException) as cm:
            self.clean_loop()

        self.assertEqual(n2.processed, 1, "Sub Channel not working")

    def test_cond_channel(self):
        """ Whether Conditionnal channel is working """

        chan = BaseChannel(name="test_channel5", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")

        msg = generate_msg()

        chan.add(n1)

        # Nodes in this channel should not be processed
        cond1 = chan.when(lambda x: False, name="Toto")
        # Nodes in this channel should be processed
        cond2 = chan.when(True, name="condchannel")

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Cond Channel when condition == False not working")
        self.assertTrue(processed.processed, "Cond Channel when condition == True not working")
        self.assertFalse(n2.processed, "Cond Channel don't became the main path")
        self.assertEqual(cond2.name, "test_channel5.condchannel", "Condchannel name is incorrect")

    def test_case_channel(self):
        """ Whether Conditionnal channel is working """

        chan = BaseChannel(name="test_channel6", loop=self.loop)
        n1 = TestNode(name="main")
        n2 = TestNode(name="end_main")
        not_processed = TestNode(name="cond_notproc")
        processed = TestNode(name="cond_proc")
        not_processed2 = TestNode(name="cond_proc2")

        msg = generate_msg()

        chan.add(n1)

        cond1, cond2, cond3 = chan.case(lambda x: False, True, True, names=['first', 'second', 'third'])

        chan.add(n2)

        cond1.add(not_processed)
        cond2.add(processed)
        cond3.add(not_processed2)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertFalse(not_processed.processed, "Case Channel when condition == False not working")
        self.assertFalse(not_processed2.processed, "Case Channel when condition == False not working")
        self.assertTrue(processed.processed, "Case Channel when condition == True not working")
        self.assertTrue(n2.processed, "Cond Channel don't became the main path")
        self.assertEqual(cond1.name, "test_channel6.first", "Casechannel name is incorrect")
        self.assertEqual(cond2.name, "test_channel6.second", "Casechannel name is incorrect")
        self.assertEqual(cond3.name, "test_channel6.third", "Casechannel name is incorrect")


    def test_channel_subchannel(self):
        """ Whether BaseChannel subchannel works """
        chan = BaseChannel(name="test_channel6.5", loop=self.loop)

        chan_fork = chan.fork()

        chan_when = chan_fork.when(lambda: True)

        chan_case1, chan_case2 = chan_when.case(lambda: True, lambda: False)

        print(chan.subchannels())

        self.assertEqual(len(chan.subchannels()[0]['subchannels'][0]['subchannels']), 2, "Subchannel graph not working")


    def test_channel_result(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        # Launch channel processing
        self.start_channels()
        result = self.loop.run_until_complete(chan.handle(msg))

        self.assertEqual(result.payload, msg.payload, "Channel handle not working")

    def test_channel_with_generator(self):
        """ Whether BaseChannel with generator is working """

        chan = BaseChannel(name="test_channel7.3", loop=self.loop)
        chan2 = BaseChannel(name="test_channel7.31", loop=self.loop)
        msg = generate_msg()
        msg2 = msg.copy()

        class TestIter(nodes.BaseNode):
            def process(self, msg):
                def iter():
                    for i in range(3):
                        yield msg
                return iter()

        final_node = nodes.Log()
        mid_node = nodes.Log()

        chan.add(TestIter(name="testiterr"), nodes.Log(), TestIter(name="testiterr"), final_node)
        chan2.add(TestIter(name="testiterr"), mid_node, nodes.Drop())

        # Launch channel processing
        self.start_channels()
        result = self.loop.run_until_complete(chan.handle(msg))

        self.assertEqual(result.payload, msg.payload, "Generator node not working")
        self.assertEqual(final_node.processed, 9, "Generator node not working")

        result = self.loop.run_until_complete(chan2.handle(msg2))

        self.assertEqual(mid_node.processed, 3, "Generator node not working with drop_node")


    def test_channel_events(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7.5", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        state_sequence = [chan.status]

        @events.channel_change_state.receiver
        def handle_change_state(channel=None, old_state=None, new_state=None):
            print(channel.name, old_state, new_state)
            state_sequence.append(new_state)

        @events.channel_change_state.receiver
        async def handle_change_state_async(channel=None, old_state=None, new_state=None):
            print(channel.name, old_state, new_state)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.stop())

        print(state_sequence)

        valid_sequence = [BaseChannel.STOPPED, BaseChannel.STARTING, BaseChannel.WAITING,
                          BaseChannel.PROCESSING, BaseChannel.WAITING, BaseChannel.STOPPING, BaseChannel.STOPPED]
        self.assertEqual(state_sequence, valid_sequence, "Sequence state is not valid")


    @mock.patch('socket.socket')
    def test_http_channel(self, mock_sock):
        """ Whether HTTPChannel is working"""
        tests = [
            dict(out_params={'sock':'localhost:8080'}),
            dict(
                in_params={'address':'localhost', 'port':8081}, 
                out_params={'sock':'localhost:8081'},
            ),
            dict(
                in_params={'address':'localhost', 'port':8081, 'host':'0.0.0.0:8082'},
                out_params={'raising':True},
            ),
            dict(
                in_params={'host':'0.0.0.0:8082', 'sock':'place_holder'},
                out_params={'raising':True},
                comment="either socket or host",
            ),
            dict(
                in_params={'address':'localhost', 'port':8081, 'sock':'place_holder'},
                out_params={'raising':True},
                comment="either addr,port or sock",
            ),
            dict(
                in_params={'address':'0.0.0.0'},
                out_params={'sock':'0.0.0.0:8080'},
                comment="dflt_port 8080"
            ),
            dict(
                in_params={'port':8081},
                out_params={'sock':'localhost:8081'},
                comment="dflt addr localhost",
            ),
            dict(
                in_params={'host':'0.0.0.0:8082', 'reuse_port':True},
                out_params={'sock':'0.0.0.0:8082'},
            ),
        ]

        fake_socket = mock.MagicMock()
        mock_sock.return_value = fake_socket
        for test in tests:
            in_params = test.get('in_params',{})
            out_params = test.get('out_params',{})
            comment = test.get('comment',"")
            raising = out_params.pop('raising', False)

            adress = in_params.get('adress')
            address = in_params.get('address')
            port = in_params.get('port')
            host = in_params.get('host')
            sock = in_params.get('sock')
            http_args =  in_params.get('http_args')
            reuse_port = in_params.get('reuse_port')

            def mk_endp():
                endp = endpoints.HTTPEndpoint(
                    loop=self.loop, 
                    adress=adress, address=address, port=port, 
                    http_args=http_args,
                    host=host,
                    sock=sock,
                    reuse_port=reuse_port
                )
                endp.make_socket()
                return endp

            check_msg = "%s: %s -> %s" % (comment, in_params, out_params)
            print(check_msg)
            if raising:
                self.assertRaises(PypemanParamError, mk_endp)
                continue
            endp = mk_endp()

            if isinstance(endp.sock, str):
                assert mock_sock.called
                sock_params = out_params.get('sock', 'localhost:8080')
                sock_host, sock_port = sock_params.split(":")
                assert fake_socket.bind.called_with(sock_host, sock_port)

            if in_params.get('reuse_port'):
                assert fake_socket.setsockopt(SOL_SOCKET, 15, 1)

            for key, value in out_params.items():
                self.assertEqual(getattr(endp, key), value, check_msg)

            chan = channels.HttpChannel(endpoint=endp, loop=self.loop)    
            

    def test_ftp_channel(self):
        """ Whether FTPWatcherChannel is working"""

        ftp_config = dict(host="fake", port=22, credentials=("fake", "fake"))

        fake_ftp = mock.MagicMock()

        mock_list_dir = mock.Mock(return_value=set(["file1", "file2"]))

        # This hack avoid bug : https://bugs.python.org/issue25599#msg256903
        def fake_list_dir(*args):
            return mock_list_dir(*args)

        fake_ftp.list_dir = fake_list_dir
        fake_ftp.download_file = mock.Mock(return_value=b"new_content")

        fake_ftp_helper = mock.Mock(return_value=fake_ftp)

        with mock.patch('pypeman.contrib.ftp.FTPHelper', new=fake_ftp_helper) as mock_ftp:
            chan = channels.FTPWatcherChannel(name="ftpchan", regex=".*", loop=self.loop,
                                              basedir="testdir", # delete_after=True,
                                              **ftp_config)

            chan.watch_for_file = asyncio.coroutine(mock.Mock())

            n = nodes.Log(name="test_ftp_chan")
            chan.add(n)
            n._reset_test()

            self.start_channels()

            self.loop.run_until_complete(chan.tick())

            self.clean_loop()

            mock_list_dir.assert_called_once_with("testdir")

            fake_ftp.download_file.assert_any_call("testdir/file1")
            fake_ftp.download_file.assert_called_with("testdir/file2")

            # TODO Delete should be tested with a fixed version of run in executor
            # otherwise we fall in bug : https://bugs.python.org/issue25599#msg256903
            # fake_ftp.delete.assert_called_with("testdir/file2")

            self.assertEqual(n.last_input().payload, b"new_content")

            # Second tick. Should do nothing.

            fake_ftp.download_file.reset_mock()
            mock_list_dir.reset_mock()

            self.loop.run_until_complete(chan.tick())

            self.clean_loop()

            mock_list_dir.assert_called_once_with("testdir")
            fake_ftp.download_file.assert_not_called()

            # Third tick. Should download a new file.

            mock_list_dir.return_value=set(["file1", "file2", "file3"])

            fake_ftp.download_file.reset_mock()
            mock_list_dir.reset_mock()

            self.loop.run_until_complete(chan.tick())

            self.clean_loop()

            mock_list_dir.assert_called_once_with("testdir")
            fake_ftp.download_file.assert_called_once_with("testdir/file3")


            # To avoid auto launch of ftp watch
            channels.all.remove(chan)

            del chan

    def test_channel_stopped_dont_process_message(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7.7", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.stop())

        with self.assertRaises(channels.ChannelStopped) as cm:
            self.loop.run_until_complete(chan.handle(msg))

    def test_channel_exception(self):
        """ Whether BaseChannel handling return an exception in case of error in main branch """

        chan = BaseChannel(name="test_channel8", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson(), ExceptNode())

        # Launch channel processing
        self.start_channels()
        with self.assertRaises(TestException) as cm:
            self.loop.run_until_complete(chan.process(msg))


