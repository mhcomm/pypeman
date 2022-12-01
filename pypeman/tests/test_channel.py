import asyncio

from pathlib import Path
from socket import SOL_SOCKET
from unittest import mock

from pypeman import channels, endpoints
from pypeman import nodes
from pypeman import events
from pypeman.channels import BaseChannel, Dropped, Rejected
from pypeman.errors import PypemanParamError
from pypeman.helpers.aio_compat import awaitify
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import ExceptNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.common import TstNode


def raise_dropped(msg):
    raise Dropped()


def raise_rejected(msg):
    raise Rejected()


def raise_exc(msg):
    raise Exception()


class ChannelsTests(TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks  # TODO: remove ?
        pending = asyncio.all_tasks(loop=self.loop)

        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending))

    def start_channels(self):
        # Start channels
        for chan in channels.all_channels:
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
        channels.all_channels.clear()

    def tearDown(self):
        super().tearDown()
        self.clean_loop()

    def test_base_channel(self):
        """ Whether BaseChannel handling is working """

        chan = BaseChannel(name="test_channel1", loop=self.loop)
        n = TstNode()
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

    def test_done_callback(self):
        """ Whether BaseChannel done_callback is working """
        chan1 = BaseChannel(name="test_channel_done_clbk", loop=self.loop)
        n1 = TstNode()
        n_callback = TstNode()
        chan1.add(n1)
        chan1.add_done_callback(n_callback)
        msg1 = generate_msg()
        endmsg = generate_msg(message_content="endmsg")
        n1.mock(output=endmsg)
        self.start_channels()
        n_callback._reset_test()
        self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(n_callback.processed, "Channel done_callback not working")
        self.assertDictEqual(
            vars(endmsg), vars(n_callback.last_input()),
            "Channel done_callback don't takes last result in input")

    def test_drop_callback(self):
        """ Whether BaseChannel drop_callback is working """
        chan1 = BaseChannel(name="test_channel_drop_clbk", loop=self.loop)
        n1 = TstNode()
        n_callback = TstNode()
        chan1.add(n1)
        chan1.add_drop_callback(n_callback)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_dropped)
        self.start_channels()
        n_callback._reset_test()
        with self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(n_callback.processed, "Channel drop_callback not working")
        self.assertDictEqual(
            vars(msg1), vars(n_callback.last_input()),
            "Channel drop_callback don't takes event msg in input")

    def test_reject_callback(self):
        """ Whether BaseChannel reject_callback is working """
        chan1 = BaseChannel(name="test_channel_reject_clbk", loop=self.loop)
        n1 = TstNode()
        n_callback = TstNode()
        chan1.add(n1)
        chan1.add_reject_callback(n_callback)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_rejected)
        self.start_channels()
        n_callback._reset_test()
        with self.assertRaises(Rejected):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(n_callback.processed, "Channel reject_callback not working")
        self.assertDictEqual(
            vars(msg1), vars(n_callback.last_input()),
            "Channel reject_callback don't takes event msg in input")

    def test_fail_callback(self):
        """ Whether BaseChannel fail_callback is working """
        chan1 = BaseChannel(name="test_channel_fail_clbk", loop=self.loop)
        n1 = TstNode()
        n_callback = TstNode()
        chan1.add(n1)
        chan1.add_fail_callback(n_callback)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_exc)
        self.start_channels()
        n_callback._reset_test()
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(n_callback.processed, "Channel fail_callback not working")
        self.assertDictEqual(
            vars(msg1), vars(n_callback.last_input()),
            "Channel fail_callback don't takes event msg in input")

    def test_multiple_callbacks(self):
        """
            Whether BaseChannel all callbacks are working at same time
        """
        chan1 = BaseChannel(name="test_channel_all_clbk", loop=self.loop)
        n1 = TstNode()
        n_callbackdone = TstNode()
        n_callbackdrop = TstNode()
        n_callbackfail = TstNode()
        n_callbackreject = TstNode()
        chan1.add(n1)
        chan1.add_reject_callback(n_callbackreject)
        chan1.add_fail_callback(n_callbackfail)
        chan1.add_drop_callback(n_callbackdrop)
        chan1.add_done_callback(n_callbackdone)
        msg1 = generate_msg(message_content="startmsg")

        # Test with ok output
        self.start_channels()
        n_callbackreject._reset_test()
        n_callbackfail._reset_test()
        n_callbackdrop._reset_test()
        n_callbackdone._reset_test()
        self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_callbackdone.processed,
            "Channel done_callback not working with other callbacks")
        self.assertFalse(
            n_callbackreject.processed,
            "Channel reject_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackdrop.processed,
            "Channel drop_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackfail.processed,
            "Channel fail_callback called when nobody ask to him")

        # Test with an exception
        n1.mock(output=raise_exc)
        self.start_channels()
        n_callbackreject._reset_test()
        n_callbackfail._reset_test()
        n_callbackdrop._reset_test()
        n_callbackdone._reset_test()
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_callbackfail.processed,
            "Channel fail_callback not working with other callbacks")
        self.assertFalse(
            n_callbackreject.processed,
            "Channel reject_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackdrop.processed,
            "Channel drop_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackdone.processed,
            "Channel done_callback called when nobody ask to him")

        # Test with a drop
        n1.mock(output=raise_dropped)
        self.start_channels()
        n_callbackreject._reset_test()
        n_callbackfail._reset_test()
        n_callbackdrop._reset_test()
        n_callbackdone._reset_test()
        with self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_callbackdrop.processed,
            "Channel drop_callback not working with other callbacks")
        self.assertFalse(
            n_callbackreject.processed,
            "Channel reject_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackfail.processed,
            "Channel fail_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackdone.processed,
            "Channel done_callback called when nobody ask to him")

        # Test with a rejected
        n1.mock(output=raise_rejected)
        self.start_channels()
        n_callbackreject._reset_test()
        n_callbackfail._reset_test()
        n_callbackdrop._reset_test()
        n_callbackdone._reset_test()
        with self.assertRaises(Rejected):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_callbackreject.processed,
            "Channel reject_callback not working with other callbacks")
        self.assertTrue(
            n_callbackfail.processed,
            "Channel fail_callback not working with other callbacks")
        self.assertFalse(
            n_callbackdrop.processed,
            "Channel drop_callback called when nobody ask to him")
        self.assertFalse(
            n_callbackdone.processed,
            "Channel done_callback called when nobody ask to him")

    def test_subchan_callbacks(self):
        """
            Whether callbacks are working correctly in complex channels and subchannels
        """
        chan1 = BaseChannel(name="test_subchannel_clbk", loop=self.loop)
        n1 = TstNode(name="n1")
        chan1.add(n1)
        subchan1 = chan1.fork(name="sub1")
        subchan2 = subchan1.fork(name="sub2")
        nsub1 = TstNode(name="nsub1")
        nsub2 = TstNode(name="nsub2")
        subchan2.add(nsub2)
        subchan1.add(nsub1)
        nsub1_endmsg = generate_msg(message_content="nsub1_endmsg")
        nsub1.mock(output=nsub1_endmsg)

        subchan3 = subchan1.fork(name="sub3")
        nsub_exc = TstNode(name="nsubexc")
        nsub_exc.mock(output=raise_exc)
        subchan3.add(nsub_exc)
        nsub3 = TstNode(name="nsub3")
        subchan3.add(nsub3)

        n2 = TstNode(name="n2")
        chan1.add(n2)
        n2_endmsg = generate_msg(message_content="n2_endmsg")
        n2.mock(output=n2_endmsg)
        subchan4 = chan1.fork(name="sub4")
        nsub_drop = TstNode(name="nsub_drop")
        nsub_drop.mock(output=raise_dropped)
        subchan4.add(nsub_drop)

        n3 = TstNode(name="n3")
        chan1.add(n3)
        n3_endmsg = generate_msg(message_content="n3_endmsg")
        n3.mock(output=n3_endmsg)

        chan1_callbackdone = TstNode(name="chan1_callbackdone")
        chan1_callbackdrop = TstNode(name="chan1_callbackdrop")
        chan1_callbackfail = TstNode(name="chan1_callbackfail")
        chan1_callbackreject = TstNode(name="chan1_callbackreject")
        chan1.add_reject_callback(chan1_callbackreject)
        chan1.add_fail_callback(chan1_callbackfail)
        chan1.add_drop_callback(chan1_callbackdrop)
        chan1.add_done_callback(chan1_callbackdone)

        sub2_callbackdone1 = TstNode(name="sub2_callbackdone1")
        sub2_callbackdone1._reset_test()
        sub2_cbk1_endmsg = generate_msg(message_content="sub2_cbk1_endmsg")
        sub2_callbackdone1.mock(output=sub2_cbk1_endmsg)
        sub2_callbackdone2 = TstNode(name="sub2_callbackdone2")
        sub2_callbackdone2._reset_test()
        sub2_callbackfail = TstNode(name="sub2_callbackfail")
        subchan2.add_fail_callback(sub2_callbackfail)
        subchan2.add_done_callback(sub2_callbackdone1, sub2_callbackdone2)

        sub3_callbackdone = TstNode(name="sub3_callbackdone")
        sub3_callbackfail = TstNode(name="sub3_callbackfail")
        sub3_callbackfail._reset_test()
        subchan3.add_fail_callback(sub3_callbackfail)
        subchan3.add_done_callback(sub3_callbackdone)

        sub4_callbackdone = TstNode(name="sub4_callbackdone")
        sub4_callbackdrop = TstNode(name="sub4_callbackdrop")
        sub4_callbackdrop._reset_test()
        sub4_callbackfail = TstNode(name="sub4_callbackfail")
        subchan4.add_fail_callback(sub4_callbackfail)
        subchan4.add_drop_callback(sub4_callbackdrop)
        subchan4.add_done_callback(sub4_callbackdone)

        startmsg = generate_msg(message_content="startmsg")
        self.start_channels()
        with self.assertRaises(Exception) and self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(startmsg))
        # except Exception:
        #     pass

        # chan1 : only done_clbk have to be called
        self.assertTrue(
            chan1_callbackdone.processed,
            "chan1 done_callback not called")
        self.assertFalse(
            chan1_callbackfail.processed,
            "chan1 fail_callback called when nobody ask to him")
        self.assertFalse(
            chan1_callbackdrop.processed,
            "chan1 drop_callback called when nobody ask to him")
        self.assertFalse(
            chan1_callbackreject.processed,
            "chan1 rejected_callback called when nobody ask to him")

        # subchan2 : only done_clbk have to be called
        self.assertTrue(
            sub2_callbackdone1.processed,
            "subchan2 done_callback1 not called")
        self.assertDictEqual(
            vars(startmsg), vars(sub2_callbackdone1.last_input()),
            "subchan2 done_callback don't takes event msg in input")
        self.assertDictEqual(
            vars(sub2_cbk1_endmsg), vars(sub2_callbackdone2.last_input()),
            "subchan2 done_callback don't takes event msg in input")
        self.assertTrue(
            sub2_callbackdone2.processed,
            "subchan2 done_callback2 not called")
        self.assertFalse(
            sub2_callbackfail.processed,
            "subchan2 fail_callback called when nobody ask to him")

        # subchan3 : only fail_clbk have to be called
        self.assertTrue(
            sub3_callbackfail.processed,
            "subchan3 fail_callback not called")
        self.assertDictEqual(
            vars(nsub1_endmsg), vars(sub3_callbackfail.last_input()),
            "subchan3 fail_callback don't takes correct input")
        self.assertFalse(
            sub3_callbackdone.processed,
            "subchan3 done_callback called when nobody ask to him")

        # subchan4 : only drop_clbk have to be called
        self.assertTrue(
            sub4_callbackdrop.processed,
            "subchan4 fail_callback not called")
        self.assertDictEqual(
            vars(n2_endmsg), vars(sub4_callbackdrop.last_input()),
            "subchan4 drop_callback don't takes correct input")
        self.assertFalse(
            sub4_callbackdone.processed,
            "subchan4 done_callback called when nobody ask to him")
        self.assertFalse(
            sub4_callbackdone.processed,
            "subchan4 fail_callback called when nobody ask to him")

    def test_sub_channel(self):
        """ Whether Sub Channel is working """

        chan = BaseChannel(name="test_channel3", loop=self.loop)
        n1 = TstNode(name="main")
        n2 = TstNode(name="sub")
        n3 = TstNode(name="sub1")
        n4 = TstNode(name="sub2")

        msg = generate_msg()

        same_chan = chan.append(n1)

        self.assertEqual(chan, same_chan, "Append don't return channel.")

        sub = chan.fork(name="subchannel")
        sub.append(n2)
        sub2 = sub.fork(name="subsubchannel")
        sub2.append(n3)
        sub.append(n4)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n2.processed, "Sub Channel not working")
        self.assertTrue(n3.processed, "Sub Channel not working")
        self.assertTrue(n4.processed, "Sub Channel not working")
        self.assertEqual(sub.name, "test_channel3.subchannel", "Subchannel name is incorrect")
        self.assertEqual(sub2.name, "test_channel3.subchannel.subsubchannel",
                         "Subchannel name is incorrect")

    def test_sub_channel_with_exception(self):
        """ Whether Sub Channel exception handling is working """

        chan = BaseChannel(name="test_channel4", loop=self.loop)
        n1 = TstNode(name="main")
        n2 = TstNode(name="sub")
        n3 = ExceptNode(name="exc")
        n4 = TstNode(name="submain")
        n5 = TstNode(name="sub2")

        msg = generate_msg()

        chan.add(n1)
        sub = chan.fork(name="Hello")
        sub.add(n2, n3)
        chan.add(n4)
        sub2 = chan.fork(name="sub2")
        sub2.append(n5)

        # Launch channel processing
        self.start_channels()

        with self.assertRaises(TstException):
            self.loop.run_until_complete(chan.handle(msg))

        self.assertEqual(n1.processed, 1, "Sub Channel not working")

        self.assertEqual(n2.processed, 1, "Sub Channel not working")
        self.assertEqual(n4.processed, 1, "Sub Channel not working")
        self.assertEqual(n5.processed, 1, "Sub Channel not working")

    def test_cond_channel(self):
        """ Whether Conditionnal channel is working """

        chan = BaseChannel(name="test_channel5", loop=self.loop)
        n1 = TstNode(name="main")
        n2 = TstNode(name="end_main")
        not_processed = TstNode(name="cond_notproc")
        processed = TstNode(name="cond_proc")

        msg = generate_msg()

        chan.add(n1)

        # Nodes in this channel should not be processed
        cond1 = chan.when(lambda x: False)
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
        n1 = TstNode(name="main")
        n2 = TstNode(name="end_main")
        not_processed = TstNode(name="cond_notproc")
        processed = TstNode(name="cond_proc")
        not_processed2 = TstNode(name="cond_proc2")

        msg = generate_msg()

        chan.add(n1)

        cond1, cond2, cond3 = chan.case(
            lambda x: False, True, True, names=['first', 'second', 'third']
        )

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

        chan_fork = chan.fork("subchannel6.5")

        chan_when = chan_fork.when(lambda: True, name="condchannel6.5.1")

        chan_case1, chan_case2 = chan_when.case(
            lambda: True, lambda: False,
            names=["condchannel6.5.2", "condchannel6.5.3"])

        print(chan.subchannels())

        self.assertEqual(
            len(chan.subchannels()[0]['subchannels'][0]['subchannels']),
            2, "Subchannel graph not working")

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

        chan.add(TestIter(name="testiterr1"), nodes.Log(), TestIter(name="testiterr2"), final_node)
        chan2.add(TestIter(name="testiterr3"), mid_node, nodes.Drop())

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
                          BaseChannel.PROCESSING, BaseChannel.WAITING,
                          BaseChannel.STOPPING, BaseChannel.STOPPED]
        self.assertEqual(state_sequence, valid_sequence, "Sequence state is not valid")

    @mock.patch('socket.socket')
    def test_http_channel(self, mock_sock):
        """ Whether HTTPChannel is working"""
        tests = [
            dict(out_params={'sock': '127.0.0.1:8080'}),
            dict(
                in_params={'host': 'localhost:8081'},
                out_params={'sock': 'localhost:8081'},
            ),
            dict(
                in_params={'address': 'localhost', 'port': 8081, 'host': '0.0.0.0:8082'},
                out_params={'raising': True},
            ),
            dict(
                in_params={'host': '0.0.0.0:8082', 'sock': 'place_holder'},
                out_params={'raising': True},
                comment="either socket or host",
            ),
            dict(
                in_params={'address': 'localhost', 'port': 8081, 'sock': 'place_holder'},
                out_params={'raising': True},
                comment="either addr,port or sock",
            ),
            dict(
                in_params={'host': '0.0.0.0'},
                out_params={'sock': '0.0.0.0:8080'},
                comment="dflt_port 8080"
            ),
            dict(
                in_params={'host': ":8081"},
                out_params={'sock': '127.0.0.1:8081'},
                comment="dflt addr 127.0.0.1",
            ),
            dict(
                in_params={'host': '0.0.0.0:8082', 'reuse_port': True},
                out_params={'sock': '0.0.0.0:8082'},
            ),
        ]

        fake_socket = mock.MagicMock()
        mock_sock.return_value = fake_socket
        test_idx = 0
        for test in tests:
            test_idx += 1
            in_params = test.get('in_params', {})
            out_params = test.get('out_params', {})
            comment = test.get('comment', "")
            raising = out_params.pop('raising', False)

            adress = in_params.get('adress')
            address = in_params.get('address')
            port = in_params.get('port')
            host = in_params.get('host')
            sock = in_params.get('sock')
            http_args = in_params.get('http_args')
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

            channels.HttpChannel(endpoint=endp, name=f"HTTPChannel{test_idx}", loop=self.loop)

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

        with mock.patch('pypeman.contrib.ftp.FTPHelper', new=fake_ftp_helper):
            chan = channels.FTPWatcherChannel(name="ftpchan", regex=".*", loop=self.loop,
                                              basedir="testdir",  # delete_after=True,
                                              **ftp_config)

            chan.watch_for_file = awaitify(mock.Mock())  # TODO: use AsyncMock when py3.8+

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

            mock_list_dir.return_value = set(["file1", "file2", "file3"])

            fake_ftp.download_file.reset_mock()
            mock_list_dir.reset_mock()

            self.loop.run_until_complete(chan.tick())

            self.clean_loop()

            mock_list_dir.assert_called_once_with("testdir")
            fake_ftp.download_file.assert_called_once_with("testdir/file3")

            # To avoid auto launch of ftp watch
            channels.all_channels.remove(chan)

            del chan

        # Basic test with extension changer
        mock_list_dir2 = mock.Mock(return_value=set(["file1.ok", "file1.txt"]))
        fake_ftp2 = mock.MagicMock()

        # This hack avoid bug : https://bugs.python.org/issue25599#msg256903
        def fake_list_dir2(*args):
            return mock_list_dir2(*args)

        fake_ftp2.list_dir = fake_list_dir2
        fake_ftp_helper2 = mock.Mock(return_value=fake_ftp2)

        with mock.patch('pypeman.contrib.ftp.FTPHelper', new=fake_ftp_helper2):
            chan2 = channels.FTPWatcherChannel(name="ftpchan2", regex=r".*\.ok$", loop=self.loop,
                                               basedir="testdir",  real_extensions=[".txt"],
                                               **ftp_config)
            n = nodes.Log(name="test_ftp_chan2")
            chan2.add(n)
            chan2.watch_for_file = asyncio.coroutine(mock.Mock())
            self.start_channels()
            self.loop.run_until_complete(chan2.tick())
            fake_ftp2.download_file.assert_called_once_with("testdir/file1.txt")
            self.clean_loop()
            channels.all_channels.remove(chan2)

            del chan2

    def test_fwatcher_channel(self):
        ftest_dir = Path(__file__).parent / "data"
        ok_fpath = ftest_dir / "testfile.ok"
        chan = channels.FileWatcherChannel(name="fwatchan", regex=r".*\.ok$", loop=self.loop,
                                           basedir=str(ftest_dir), real_extensions=[".txt"])
        n = nodes.Log(name="test_fwatch_chan")
        chan.add(n)
        n._reset_test()
        self.start_channels()
        ok_fpath.touch()
        self.loop.run_until_complete(chan.watch_for_file())
        self.assertEqual(n.last_input().payload, "testfilecontent")
        self.clean_loop()

    def test_channel_stopped_dont_process_message(self):
        """ Whether BaseChannel handling return a good result """

        chan = BaseChannel(name="test_channel7.7", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson())

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.stop())

        with self.assertRaises(channels.ChannelStopped):
            self.loop.run_until_complete(chan.handle(msg))

    def test_channel_exception(self):
        """ Whether BaseChannel handling return an exception in case of error in main branch """

        chan = BaseChannel(name="test_channel8", loop=self.loop)
        msg = generate_msg()

        chan.add(nodes.JsonToPython(), nodes.PythonToJson(), ExceptNode())

        # Launch channel processing
        self.start_channels()
        with self.assertRaises(TstException):
            self.loop.run_until_complete(chan.process(msg))
