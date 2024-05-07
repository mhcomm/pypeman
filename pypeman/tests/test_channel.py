import asyncio
import logging
import shutil
import tempfile
import time

from hl7.client import MLLPClient

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
from pypeman.tests.common import MllPChannelTestThread
from pypeman.tests.common import TstException
from pypeman.tests.common import TstNode


logger = logging.getLogger(__name__)


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

    def clean_msg(self, msg):
        # rm chan_rslt , chan_exc and chan_exc_traceback attributes from msg
        # if they exists because it's difficult to compare msg what contains
        # reference to exception or other msgs
        if hasattr(msg, "chan_rslt"):
            del msg.chan_rslt
        if hasattr(msg, "chan_exc"):
            del msg.chan_exc
        if hasattr(msg, "chan_exc_traceback"):
            del msg.chan_exc_traceback
        return msg

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

        for end in endpoints.all_endpoints:
            self.loop.run_until_complete(end.stop())

        # Stop all channels
        for chan in channels.all_channels:
            if not chan.is_stopped:
                self.loop.run_until_complete(chan.stop())
        endpoints.reset_pypeman_endpoints()

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

    def test_join_nodes(self):
        """ Whether BaseChannel join_nodes is working """
        chan1 = BaseChannel(name="test_channel_done_clbk", loop=self.loop)
        n1 = TstNode()
        endnode = TstNode()
        chan1.add(n1)
        chan1.add_join_nodes(endnode)
        msg1 = generate_msg()
        endmsg = generate_msg(message_content="endmsg")
        n1.mock(output=endmsg)
        self.start_channels()
        endnode._reset_test()
        self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(endnode.processed, "Channel ok_endnodes not working")
        self.assertDictEqual(
            vars(endmsg), vars(endnode.last_input()),
            "Channel ok_endnodes don't takes last result in input")

    def test_drop_nodes(self):
        """ Whether BaseChannel drop_nodes is working """
        chan1 = BaseChannel(name="test_channel_drop_clbk", loop=self.loop)
        n1 = TstNode()
        endnode = TstNode()
        chan1.add(n1)
        chan1.add_drop_nodes(endnode)
        chan1._reset_test()
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_dropped)
        self.start_channels()
        with self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(endnode.processed, "Channel drop_endnodes not working")
        endnode_input = endnode.last_input()
        self.assertFalse(endnode_input.chan_rslt,
                         "Channel drop_nodes have rslt attr when it doesn't have to")
        self.assertTrue(endnode_input.chan_exc, "Channel drop_nodes doesn't have exc as msg attr")
        self.assertTrue(endnode_input.chan_exc_traceback,
                        "Channel drop_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(msg1)), vars(self.clean_msg(endnode_input)),
            "Channel drop_endnodes don't takes event msg in input")

    def test_reject_nodes(self):
        """ Whether BaseChannel reject_nodes is working """
        chan1 = BaseChannel(name="test_channel_reject_clbk", loop=self.loop)
        n1 = TstNode()
        endnode = TstNode()
        chan1.add(n1)
        chan1.add_reject_nodes(endnode)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_rejected)
        self.start_channels()
        endnode._reset_test()
        with self.assertRaises(Rejected):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(endnode.processed, "Channel reject_endnodes not working")
        endnode_input = endnode.last_input()
        self.assertFalse(endnode_input.chan_rslt,
                         "Channel reject_nodes have rslt attr when it doesn't have to")
        self.assertTrue(endnode_input.chan_exc, "Channel reject_nodes doesn't have exc as msg attr")
        self.assertTrue(endnode_input.chan_exc_traceback,
                        "Channel reject_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(msg1)), vars(self.clean_msg(endnode_input)),
            "Channel reject_endnodes don't takes event msg in input")

    def test_fail_nodes(self):
        """ Whether BaseChannel fail_nodes is working """
        chan1 = BaseChannel(name="test_channel_fail_clbk", loop=self.loop)
        n1 = TstNode()
        endnode = TstNode()
        chan1.add(n1)
        chan1.add_fail_nodes(endnode)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_exc)
        self.start_channels()
        endnode._reset_test()
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(endnode.processed, "Channel fail_endnodes not working")
        endnode_input = endnode.last_input()
        self.assertFalse(endnode_input.chan_rslt,
                         "Channel fail_nodes have rslt attr when it doesn't have to")
        self.assertTrue(endnode_input.chan_exc, "Channel fail_nodes doesn't have exc as msg attr")
        self.assertTrue(endnode_input.chan_exc_traceback,
                        "Channel fail_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(msg1)), vars(self.clean_msg(endnode_input)),
            "Channel fail_nodes don't takes event msg in input")

    def test_final_nodes(self):
        """ Whether BaseChannel final_nodes is working """
        chan1 = BaseChannel(name="test_channel_final_clbk", loop=self.loop)
        n1 = TstNode()
        endnode = TstNode()
        chan1.add(n1)
        chan1.add_final_nodes(endnode)
        msg1 = generate_msg(message_content="startmsg")
        n1.mock(output=raise_exc)
        self.start_channels()
        endnode._reset_test()
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(msg1))

        self.assertTrue(endnode.processed, "Channel final_endnodes not working")
        endnode_input = endnode.last_input()
        self.assertFalse(endnode_input.chan_rslt,
                         "Channel final_nodes have rslt attr when it doesn't have to")
        self.assertTrue(endnode_input.chan_exc, "Channel final_nodes doesn't have exc as msg attr")
        self.assertTrue(endnode_input.chan_exc_traceback,
                        "Channel final_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(msg1)), vars(self.clean_msg(endnode_input)),
            "Channel final_nodes don't takes event msg in input")

    def test_multiple_callbacks(self):
        """
            Whether BaseChannel all endnodes are working at same time
        """
        chan1 = BaseChannel(name="test_channel_all_clbk", loop=self.loop)
        n1 = TstNode()
        n_endok = TstNode()
        n_enddrop = TstNode()
        n_endfail = TstNode()
        n_endreject = TstNode()
        n_endfinal = TstNode()
        chan1.add(n1)
        chan1.add_reject_nodes(n_endreject)
        chan1.add_fail_nodes(n_endfail)
        chan1.add_drop_nodes(n_enddrop)
        chan1.add_join_nodes(n_endok)
        chan1.add_final_nodes(n_endfinal)
        msg1 = generate_msg(message_content="startmsg")

        # Test with ok output
        chan1._reset_test()
        self.start_channels()
        self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_endok.processed,
            "Channel join_endnodes not working with other callbacks")
        self.assertTrue(
            n_endfinal.processed,
            "Channel final_endnodes not working with other callbacks")
        self.assertFalse(
            n_endreject.processed,
            "Channel reject_endnodes called when nobody ask to him")
        self.assertFalse(
            n_enddrop.processed,
            "Channel drop_endnodes called when nobody ask to him")
        self.assertFalse(
            n_endfail.processed,
            "Channel fail_endnodes called when nobody ask to him")

        # Test with an exception
        chan1._reset_test()
        n1.mock(output=raise_exc)
        self.start_channels()
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_endfail.processed,
            "Channel fail_endnodes not working with other callbacks")
        self.assertTrue(
            n_endfinal.processed,
            "Channel final_endnodes not working with other callbacks")
        self.assertFalse(
            n_endreject.processed,
            "Channel reject_endnodes called when nobody ask to him")
        self.assertFalse(
            n_enddrop.processed,
            "Channel drop_endnodes called when nobody ask to him")
        self.assertFalse(
            n_endok.processed,
            "Channel ok_endnodes called when nobody ask to him")

        # Test with a drop
        chan1._reset_test()
        n1.mock(output=raise_dropped)
        self.start_channels()
        with self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_enddrop.processed,
            "Channel drop_endnodes not working with other callbacks")
        self.assertTrue(
            n_endfinal.processed,
            "Channel final_endnodes not working with other callbacks")
        self.assertFalse(
            n_endreject.processed,
            "Channel reject_endnodes called when nobody ask to him")
        self.assertFalse(
            n_endfail.processed,
            "Channel fail_endnodes called when nobody ask to him")
        self.assertFalse(
            n_endok.processed,
            "Channel ok_endnodes called when nobody ask to him")

        # Test with a rejected
        chan1._reset_test()
        n1.mock(output=raise_rejected)
        self.start_channels()
        with self.assertRaises(Rejected):
            self.loop.run_until_complete(chan1.handle(msg1))
        self.assertTrue(
            n_endreject.processed,
            "Channel reject_endnodes not working with other callbacks")
        self.assertTrue(
            n_endfinal.processed,
            "Channel final_endnodes not working with other callbacks")
        self.assertFalse(
            n_endfail.processed,
            "Channel fail_endnodes not working with other callbacks")
        self.assertFalse(
            n_enddrop.processed,
            "Channel drop_endnodes called when nobody ask to him")
        self.assertFalse(
            n_endok.processed,
            "Channel ok_endnodes called when nobody ask to him")

    def test_condchan_endnodes(self):
        """
            Whether endnodes are working correctly in case subchannels
        """
        chan1 = BaseChannel(name="test_condchannel_endnodes", loop=self.loop)
        n1 = TstNode(name="n1")
        chan1.add(n1)
        condchan = chan1.when(
            lambda msg: msg.payload == "exc"
        )
        n2 = TstNode(name="n2")
        n3exc = TstNode(name="n3")
        chan1.add(n2)
        condchan.add(n3exc)

        chan1_endok = TstNode(name="chan1_endok")
        chan1_endfail = TstNode(name="chan1_endfail")
        chan1_endfinal = TstNode(name="chan1_endfinal")
        chan1.add_fail_nodes(chan1_endfail)
        chan1.add_join_nodes(chan1_endok)
        chan1.add_final_nodes(chan1_endfinal)

        condchan_end = TstNode(name="condchan_end")
        condchan.add_final_nodes(condchan_end)

        chan1._reset_test()

        # Test Msg don't enter in exc subchan
        startmsg = generate_msg(message_content="startmsg")
        self.start_channels()
        self.loop.run_until_complete(chan1.handle(startmsg))
        self.assertEqual(
            chan1_endok.processed, 1,
            "chan1 ok_endnodes not called or called multiple times")
        self.assertEqual(
            chan1_endfinal.processed, 1,
            "chan1 final_endnodes not called or called multiple times")
        self.assertEqual(
            chan1_endfail.processed, 0,
            "chan1 fail_callback called when nobody ask to him")
        self.assertEqual(
            chan1_endfail.processed, 0,
            "chan1 fail_callback called when nobody ask to him")
        self.assertEqual(
            condchan_end.processed, 0,
            "condchan1 final_callback called when nobody ask to him")

        chan1._reset_test()
        n3exc.mock(output=raise_exc)
        excmsg = generate_msg(message_content="exc")
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(excmsg))
        self.assertEqual(
            chan1_endfail.processed, 1,
            "chan1 fail_endnodes not called or called multiple times")
        self.assertEqual(
            chan1_endfinal.processed, 1,
            "chan1 final_endnodes not called or called multiple times")
        self.assertEqual(
            chan1_endok.processed, 0,
            "chan1 ok_callback called when nobody ask to him")
        self.assertEqual(
            condchan_end.processed, 1,
            "condchan1 final_callback not called or called multiple times")

    def test_casecondchan_endnodes(self):
        """
            Whether endnodes are working correctly in case subchannels
        """
        chan1 = BaseChannel(name="test_casechannel_endnodes", loop=self.loop)
        n1 = TstNode(name="n1")
        chan1.add(n1)
        condchanok1, condchanok2, condchanexc = chan1.case(
            lambda msg: msg.payload == "ok1",
            lambda msg: msg.payload == "ok2",
            lambda msg: msg.payload == "exc",
            names=["condchanok1", "condchanok2", "condchanexc"]
        )
        n2 = TstNode(name="n2")
        chan1.add(n2)
        n_ok1 = TstNode(name="n_ok1")
        nok1_endmsg = generate_msg(message_content="nok1_endmsg")
        n_ok1.mock(output=nok1_endmsg)
        condchanok1.add(n_ok1)
        n_ok2 = TstNode(name="n_ok2")
        nok2_endmsg = generate_msg(message_content="nok2_endmsg")
        n_ok2.mock(output=nok2_endmsg)
        condchanok2.add(n_ok2)
        n_exc = TstNode(name="nexc")
        n_exc.mock(output=raise_exc)
        condchanexc.add(n_exc)

        chan1_endok = TstNode(name="chan1_endok")
        chan1_endfail = TstNode(name="chan1_endfail")
        chan1_endfinal = TstNode(name="chan1_endfinal")
        chan1.add_fail_nodes(chan1_endfail)
        chan1.add_join_nodes(chan1_endok)
        chan1.add_final_nodes(chan1_endfinal)
        chan1_endfail._reset_test()
        chan1_endok._reset_test()
        chan1_endfinal._reset_test()

        # Test without entering in cond subchans
        startmsg = generate_msg(message_content="startmsg")
        self.start_channels()
        self.loop.run_until_complete(chan1.handle(startmsg))
        self.assertTrue(
            chan1_endok.processed,
            "chan1 join_nodes not called")
        self.assertTrue(
            chan1_endfinal.processed,
            "chan1 final_endnodes not called")
        self.assertFalse(
            chan1_endfail.processed,
            "chan1 fail_callback called when nobody ask to him")
        # Test that the msg_rslt attr in the msg entering in the final nodes
        # is the same that the message that came out from the chan process
        chan1_endfinal_input = chan1_endfinal.last_input()
        self.assertTrue(chan1_endfinal_input.chan_rslt,
                        "Channel final_nodes doesn't have rslt attr when it haves to")
        # Must pop msg_rslt attr, because the msg in msg_rslt doesn't
        # have itself a msg_rslt attr set
        chan1_endfinal_msg_rlst_dict = vars(chan1_endfinal_input.chan_rslt)
        chan1_endfinal_msg_rlst_dict.pop("chan_rslt")
        chan1_endfinal_input_dict = vars(chan1_endfinal_input)
        chan1_endfinal_input_dict.pop("chan_rslt")
        self.assertDictEqual(chan1_endfinal_input_dict, chan1_endfinal_msg_rlst_dict,
                             "final nodes don't have correct rslt extra data in msg")
        self.assertFalse(chan1_endfinal_input.chan_exc, "Channel final_nodes have exc as msg attr ..")
        self.assertFalse(chan1_endfinal_input.chan_exc_traceback,
                         "Channel final_nodes have exc trcbk as msg attr ..")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endok.last_input())),
            "chan join_nodes don't takes chan output in input")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endfinal_input)),
            "chan final_endnodes don't takes event msg in input")

        # Test entering in cond subchans ok1
        chan1_endfail._reset_test()
        chan1_endok._reset_test()
        chan1_endfinal._reset_test()
        startmsg = generate_msg(message_content="ok1")
        self.loop.run_until_complete(chan1.handle(startmsg))
        self.assertTrue(
            chan1_endok.processed,
            "chan1 ok_endnodes not called")
        self.assertTrue(
            chan1_endfinal.processed,
            "chan1 final_endnodes not called")
        self.assertFalse(
            chan1_endfail.processed,
            "chan1 fail_callback called when nobody ask to him")
        chan1_endfinal_input = chan1_endfinal.last_input()
        self.assertTrue(chan1_endfinal_input.chan_rslt,
                        "Channel final_nodes doesn't have rslt attr when it haves to")
        self.assertDictEqual(vars(nok1_endmsg), vars(chan1_endfinal_input.chan_rslt),
                             "final nodes don't have correct rslt extra data in msg")
        self.assertFalse(chan1_endfinal_input.chan_exc, "Channel final_nodes have exc as msg attr ..")
        self.assertFalse(chan1_endfinal_input.chan_exc_traceback,
                         "Channel final_nodes have exc trcbk as msg attr ..")
        self.assertDictEqual(
            vars(nok1_endmsg), vars(chan1_endok.last_input()),
            "chan ok_endnodes don't takes chan output in input")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endfinal_input)),
            "chan final_endnodes don't takes event msg in input")

        # Test entering in cond subchans ok2
        chan1_endfail._reset_test()
        chan1_endok._reset_test()
        chan1_endfinal._reset_test()
        startmsg = generate_msg(message_content="ok2")
        self.loop.run_until_complete(chan1.handle(startmsg))
        self.assertTrue(
            chan1_endok.processed,
            "chan1 ok_endnodes not called")
        self.assertTrue(
            chan1_endfinal.processed,
            "chan1 final_endnodes not called")
        self.assertFalse(
            chan1_endfail.processed,
            "chan1 fail_callback called when nobody ask to him")
        chan1_endfinal_input = chan1_endfinal.last_input()
        self.assertTrue(chan1_endfinal_input.chan_rslt,
                        "Channel final_nodes doesn't have rslt attr when it haves to")
        self.assertDictEqual(vars(nok2_endmsg), vars(chan1_endfinal_input.chan_rslt),
                             "final nodes don't have correct rslt extra data in msg")
        self.assertFalse(chan1_endfinal_input.chan_exc, "Channel final_nodes have exc as msg attr ..")
        self.assertFalse(chan1_endfinal_input.chan_exc_traceback,
                         "Channel final_nodes have exc trcbk as msg attr ..")
        self.assertDictEqual(
            vars(nok2_endmsg), vars(chan1_endok.last_input()),
            "chan join_nodes don't takes chan output in input")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endfinal_input)),
            "chan final_nodes don't takes event msg in input")

        # Test entering in cond subchans exc (raising exc)
        chan1_endfail._reset_test()
        chan1_endok._reset_test()
        chan1_endfinal._reset_test()
        startmsg = generate_msg(message_content="exc")
        with self.assertRaises(Exception):
            self.loop.run_until_complete(chan1.handle(startmsg))
        self.assertTrue(
            chan1_endfail.processed,
            "chan1 fail_endnodes not called")
        self.assertTrue(
            chan1_endfinal.processed,
            "chan1 final_endnodes not called")
        self.assertFalse(
            chan1_endok.processed,
            "chan1 ok_callback called when nobody ask to him")
        chan1_endfinal_input = chan1_endfinal.last_input()
        self.assertFalse(chan1_endfinal_input.chan_rslt,
                         "Channel final_nodes have rslt attr when it doesn't have to")
        self.assertTrue(chan1_endfinal_input.chan_exc, "Channel final_nodes doesn't have exc as msg attr")
        self.assertTrue(chan1_endfinal_input.chan_exc_traceback,
                        "Channel final_nodes doesn't have exc trcbk as msg attr")
        chan1_endfail_input = chan1_endfail.last_input()
        self.assertFalse(chan1_endfail_input.chan_rslt,
                         "Channel fail_nodes have rslt attr when it doesn't have to")
        self.assertTrue(chan1_endfail_input.chan_exc, "Channel fail_nodes doesn't have exc as msg attr")
        self.assertTrue(chan1_endfail_input.chan_exc_traceback,
                        "Channel fail_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endfail_input)),
            "chan fail_endnodes don't takes event msg in input")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(chan1_endfinal_input)),
            "chan final_endnodes don't takes event msg in input")

    def test_subchan_endnodes(self):
        """
            Whether endnodes are working correctly in complex channels and subchannels
        """
        chan1 = BaseChannel(name="test_subchannel_clbk", loop=self.loop, wait_subchans=True)
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

        chan1_endok = TstNode(name="chan1_endok")
        chan1_enddrop = TstNode(name="chan1_enddrop")
        chan1_endfail = TstNode(name="chan1_endfail")
        chan1_endreject = TstNode(name="chan1_endreject")
        chan1_endfinal = TstNode(name="chan1_endfinal")
        chan1_endok._reset_test()
        chan1_endfinal._reset_test()
        chan1.add_reject_nodes(chan1_endreject)
        chan1.add_fail_nodes(chan1_endfail)
        chan1.add_drop_nodes(chan1_enddrop)
        chan1.add_join_nodes(chan1_endok)
        chan1.add_final_nodes(chan1_endfinal)

        sub2_endok1 = TstNode(name="sub2_endok1")
        sub2_endok1._reset_test()
        sub2_cbk1_endmsg = generate_msg(message_content="sub2_cbk1_endmsg")
        sub2_endok1.mock(output=sub2_cbk1_endmsg)
        sub2_endok2 = TstNode(name="sub2_endok2")
        sub2_endok2._reset_test()
        sub2_endfail = TstNode(name="sub2_endfail")
        subchan2.add_fail_nodes(sub2_endfail)
        subchan2.add_join_nodes(sub2_endok1, sub2_endok2)

        sub3_endok = TstNode(name="sub3_endok")
        sub3_endfail = TstNode(name="sub3_endfail")
        sub3_endfinal = TstNode(name="sub3_endfinal")
        sub3_endfail._reset_test()
        sub3_endfinal._reset_test()
        subchan3.add_fail_nodes(sub3_endfail)
        subchan3.add_final_nodes(sub3_endfinal)
        subchan3.add_join_nodes(sub3_endok)

        sub4_endok = TstNode(name="sub4_endok")
        sub4_enddrop = TstNode(name="sub4_enddrop")
        sub4_enddrop._reset_test()
        sub4_endfail = TstNode(name="sub4_endfail")
        subchan4.add_fail_nodes(sub4_endfail)
        subchan4.add_drop_nodes(sub4_enddrop)
        subchan4.add_join_nodes(sub4_endok)

        startmsg = generate_msg(message_content="startmsg")
        self.start_channels()
        with self.assertRaises(Exception) and self.assertRaises(Dropped):
            self.loop.run_until_complete(chan1.handle(startmsg))

        # chan1 : only ok and final end nodes have to be called
        # + checks that the message that enters the final nodes is the startmsg with the
        # origin payload (not modified by other node)
        self.assertTrue(
            chan1_endok.processed,
            "chan1 ok_endnodes not called")
        self.assertTrue(
            chan1_endfinal.processed,
            "chan1 final_endnodes not called")
        self.assertFalse(
            chan1_endfail.processed,
            "chan1 fail_endnodes called when nobody ask to him")
        self.assertFalse(
            chan1_enddrop.processed,
            "chan1 drop_endnodes called when nobody ask to him")
        self.assertFalse(
            chan1_endreject.processed,
            "chan1 rejected_callback called when nobody ask to him")
        chan1_endok_input = chan1_endok.last_input()
        self.assertDictEqual(vars(n3_endmsg), vars(chan1_endok_input),
                             "ok end nodes don't have correct rslt extra data in msg")
        chan1_endfinal_input = chan1_endfinal.last_input()
        self.assertTrue(chan1_endfinal_input.chan_rslt,
                        "Channel final_nodes doesn't have rslt attr when it haves to")
        self.assertDictEqual(vars(n3_endmsg), vars(chan1_endfinal_input.chan_rslt),
                             "final nodes don't have correct rslt extra data in msg")
        self.assertEqual(startmsg.payload, chan1_endfinal_input.payload,
                         "final nodes don't have the start msg in entry")

        # subchan2 : only join nodes have to be called
        self.assertTrue(
            sub2_endok1.processed,
            "subchan2 ok_endnodes1 not called")
        self.assertDictEqual(
            vars(self.clean_msg(startmsg)), vars(self.clean_msg(sub2_endok1.last_input())),
            "subchan2 ok_endnodes don't takes event msg in input")
        self.assertDictEqual(
            vars(self.clean_msg(sub2_cbk1_endmsg)), vars(self.clean_msg(sub2_endok2.last_input())),
            "subchan2 ok_endnodes don't takes event msg in input")
        self.assertTrue(
            sub2_endok2.processed,
            "subchan2 ok_endnodes2 not called")
        self.assertFalse(
            sub2_endfail.processed,
            "subchan2 fail_endnodes called when nobody ask to him")

        # subchan3 : only fail and final endnodes have to be called
        self.assertTrue(
            sub3_endfail.processed,
            "subchan3 fail_endnodes not called")
        self.assertTrue(
            sub3_endfinal.processed,
            "subchan3 final_endnodes not called")
        self.assertFalse(
            sub3_endok.processed,
            "subchan3 ok_endnodes called when nobody ask to him")
        sub3_endfail_input = sub3_endfail.last_input()
        self.assertFalse(sub3_endfail_input.chan_rslt,
                         "subchan3 fail_nodes have rslt attr when it doesn't have to")
        self.assertTrue(sub3_endfail_input.chan_exc, "subchan3 fail_nodes doesn't have exc as msg attr")
        self.assertTrue(sub3_endfail_input.chan_exc_traceback,
                        "subchan3 fail_nodes doesn't have exc trcbk as msg attr")
        sub3_endfinal_input = sub3_endfinal.last_input()
        self.assertFalse(sub3_endfinal_input.chan_rslt,
                         "subchan3 final_nodes have rslt attr when it doesn't have to")
        self.assertTrue(sub3_endfinal_input.chan_exc, "subchan3 final_nodes doesn't have exc as msg attr")
        self.assertTrue(sub3_endfinal_input.chan_exc_traceback,
                        "subchan3 final_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(nsub1_endmsg)), vars(self.clean_msg(sub3_endfail_input)),
            "subchan3 fail_endnodes don't takes correct input")
        self.assertDictEqual(
            vars(self.clean_msg(nsub1_endmsg)), vars(self.clean_msg(sub3_endfinal_input)),
            "subchan3 final_endnodes don't takes correct input")

        # subchan4 : only drop endnodes have to be called
        self.assertFalse(
            sub4_endok.processed,
            "subchan4 ok_endnodes called when nobody ask to him")
        self.assertFalse(
            sub4_endok.processed,
            "subchan4 fail_endnodes called when nobody ask to him")
        self.assertTrue(
            sub4_enddrop.processed,
            "subchan4 fail_endnodes not called")
        sub4_enddrop_input = sub4_enddrop.last_input()
        self.assertFalse(sub4_enddrop_input.chan_rslt,
                         "subchan4 drop_nodes have rslt attr when it doesn't have to")
        self.assertTrue(sub4_enddrop_input.chan_exc, "subchan4 drop_nodes doesn't have exc as msg attr")
        self.assertTrue(sub4_enddrop_input.chan_exc_traceback,
                        "subchan4 drop_nodes doesn't have exc trcbk as msg attr")
        self.assertDictEqual(
            vars(self.clean_msg(n2_endmsg)), vars(self.clean_msg(sub4_enddrop_input)),
            "subchan4 drop_endnodes don't takes correct input")

    def test_sub_channel(self):
        """ Whether Sub Channel is working """

        chan = BaseChannel(name="test_channel3", loop=self.loop, wait_subchans=True)
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

        chan = BaseChannel(name="test_channel4", loop=self.loop, wait_subchans=True)
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
        msg = generate_msg(message_content={"test": 1})

        chan.add(nodes.PythonToJson(), nodes.JsonToPython())

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
            self.clean_loop()
            fake_ftp2.download_file.assert_called_once_with("testdir/file1.txt")
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

    def test_hl7_mllpchannel(self):
        def send_mllp(host, port, data_to_send):
            client = MLLPClient(host=host, port=port)
            client.send_message(data_to_send)
        host = "127.0.0.1"
        port = 21000
        name = "test_channelmllp0"

        # Init and start mllp server
        mllp_chan_thread = MllPChannelTestThread(chan_name=name, host=host, port=port)
        n1 = TstNode()
        mllp_chan_thread.chan.add(n1)
        mllp_chan_thread.start()
        n1._reset_test()
        ftest_dir = Path(__file__).parent / "data"
        hl7_data_fpath = ftest_dir / "hl7_test_data.HL7"
        with open(hl7_data_fpath, "r") as fin:
            hl7_strdata = fin.read()
        time.sleep(0.5)  # wait to be sure server is correctly started
        try:
            send_mllp(host, port, hl7_strdata)
        except Exception as exc:
            raise exc
        finally:
            mllp_chan_thread.kill()
            mllp_chan_thread.join()
        assert n1.last_input().payload == hl7_strdata

    def test_mergechannel(self):
        ftest_dir = Path(__file__).parent / "data"
        txt_fpath = ftest_dir / "testfile.txt"

        with tempfile.TemporaryDirectory() as tmpdirpath1:
            with tempfile.TemporaryDirectory() as tmpdirpath2:
                # Conf file watcher 1
                file_chan1_bdir = tmpdirpath1
                file_chan1_regex = r".*\.txt$"
                file_chan1_name = "test_channelfile1"

                # Conf file watcher 2
                file_chan2_bdir = tmpdirpath2
                file_chan2_regex = r".*\.txt$"
                file_chan2_name = "test_channelfile2"

                # Conf Merge Channel
                merge_chan_name = "test_channelmerge0"
                # Init channels
                file_chan1 = channels.FileWatcherChannel(
                    name=file_chan1_name,
                    basedir=file_chan1_bdir,
                    regex=file_chan1_regex,
                    interval=0.1,
                    loop=self.loop
                )
                file_chan2 = channels.FileWatcherChannel(
                    name=file_chan2_name,
                    basedir=file_chan2_bdir,
                    regex=file_chan2_regex,
                    interval=0.1,
                    loop=self.loop
                )
                merge_chan = channels.MergeChannel(
                    name=merge_chan_name, chans=[file_chan1, file_chan2], loop=self.loop)
                delete_node = nodes.FileCleaner()
                merge_chan.add(delete_node)
                merge_chan._reset_test()

                # Test that only merge chan is in all_channels
                self.assertNotIn(file_chan1, channels.all_channels)
                self.assertNotIn(file_chan2, channels.all_channels)
                self.assertIn(merge_chan, channels.all_channels)
                self.start_channels()

                time.sleep(0.1)  # wait to be sure channels are started

                self.assertEqual(merge_chan.processed_msgs, 0)

                # Test file chan1 processing
                infpath_chan1 = Path(tmpdirpath1) / txt_fpath.name
                shutil.copy(txt_fpath, infpath_chan1)
                self.loop.run_until_complete(file_chan1.watch_for_file())
                self.loop.run_until_complete(file_chan2.watch_for_file())
                self.assertEqual(merge_chan.processed_msgs, 1)
                self.assertFalse(infpath_chan1.exists())

                # Test File2 processing
                infpath_chan2 = Path(tmpdirpath2) / txt_fpath.name
                shutil.copy(txt_fpath, infpath_chan2)
                self.loop.run_until_complete(file_chan1.watch_for_file())
                self.loop.run_until_complete(file_chan2.watch_for_file())
                self.assertEqual(merge_chan.processed_msgs, 2)
                self.assertFalse(infpath_chan2.exists())
