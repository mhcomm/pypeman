
import asyncio
import json
import os
import shutil
import tempfile

from pathlib import Path

from pypeman import channels
from pypeman import message
from pypeman import msgstore
from pypeman import nodes
from pypeman.channels import BaseChannel
from pypeman.message import Message
from pypeman.test import TearDownProjectTestCase as TestCase
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.common import TstNode


class ObjectWithoutStr:
    def __str__(self):
        raise NotImplementedError("__str__ not implemented")


class TstConditionalErrorNode(nodes.BaseNode):

    def process(self, msg):
        print("Process %s" % self.name)

        if isinstance(msg.payload, str) and "shall_fail" in msg.payload:
            raise TstException()

        return msg


class MsgstoreTests(TestCase):
    def clean_loop(self):
        # Useful to execute future callbacks
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

    def test_null_message_store(self):
        """ We can store a message in NullMessageStore """

        chan = BaseChannel(
            name="test_channel9", loop=self.loop,
            message_store_factory=msgstore.FakeMessageStoreFactory())
        n = TstNode()
        msg = generate_msg(with_context=True)

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))

        self.assertTrue(n.processed, "Channel handle not working")

    def test_memory_message_store(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10", loop=self.loop, message_store_factory=store_factory)

        n = TstNode()
        n_error = TstConditionalErrorNode()

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35), message_content="message content2")
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35), message_content="message content3")
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content="message_content4")

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35),
                            message_content='{"test1": "shall_fail"}')
        msg6 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content=ObjectWithoutStr())
        msg2_uuid = msg2.uuid
        chan.add(n)
        chan.add(n_error)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))

        with self.assertRaises(TstException):
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))
        self.loop.run_until_complete(chan.handle(msg6))
        msg6 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content=ObjectWithoutStr())

        self.assertEqual(self.loop.run_until_complete(chan.message_store.total()),
                         6, "Should be a total of 6 messages in store!")

        msg_stored1 = list(self.loop.run_until_complete(chan.message_store.search(0, 2)))
        self.assertEqual(len(msg_stored1), 2, "Should be 2 results from search!")

        msg_stored2 = list(self.loop.run_until_complete(chan.message_store.search(2, 5)))
        self.assertEqual(len(msg_stored2), 4, "Should be 4 results from search!")

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))

        # All message stored ?
        self.assertEqual(len(msg_stored), 6, "Should be 6 messages in store!")

        for msg in msg_stored:
            print(msg)

        for msg in msg_stored1 + msg_stored2:
            print(msg)

        ids1 = [msg['id'] for msg in msg_stored1 + msg_stored2]
        ids2 = [msg['id'] for msg in msg_stored]

        self.assertEqual(ids1, ids2, "Should be 6 messages in store!")

        # Test processed message
        dict_msg = self.loop.run_until_complete(chan.message_store.get('%s' % msg3.uuid))
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = self.loop.run_until_complete(chan.message_store.get('%s' % msg5.uuid))
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

        # Test list messages
        msgs = self.loop.run_until_complete(chan.message_store.search(start_id=msg2_uuid, count=5))
        self.assertEqual(len(msgs), 4, "Failure of listing messages from memory msg store")

        # Test list messages with date filters
        msgs = self.loop.run_until_complete(chan.message_store.search(
            start_dt="1982-11-27", end_dt="1982-11-28T13:00:00"))
        self.assertEqual(len(msgs), 2, "Failure of listing messages from memory msg store")

        # Test list messages with text filter
        msgs = self.loop.run_until_complete(chan.message_store.search(text="sage con"))
        self.assertEqual(len(msgs), 2, "Failure of listing messages from memory msg store")

        # Test view message
        msg_content = self.loop.run_until_complete(chan.message_store.get_msg_content('%s' % msg5.uuid))
        self.assertEqual(msg_content.payload, msg5.payload, "Failure of message %s view!" % msg5)

        # Test preview message
        msg_content = self.loop.run_until_complete(chan.message_store.get_preview_str('%s' % msg5.uuid))
        self.assertEqual(msg_content.payload, msg5.payload[:1000], "Failure of message %s preview!" % msg5)

    def test_memory_message_store_store_id(self):
        """
            Test that store_id is correctly added to message's attrs and is correctly added to
            yielded sub-messages
        """
        store_factory = msgstore.MemoryMessageStoreFactory()

        # Basic chan
        chan_name = "test_channel_mem_store_id"
        chan = BaseChannel(
            name=chan_name,
            loop=self.loop,
            message_store_factory=store_factory
        )
        yieldernode = nodes.YielderNode()
        chan.add(yieldernode)

        # Second chan with no msgstore configured
        chan_name2 = "test_channel_wo_store"
        chan2_wo_msgstore = BaseChannel(
            name=chan_name2,
            loop=self.loop
        )
        chan2_wo_msgstore.add(nodes.BaseNode())

        data = [1, 2, 3]
        msg = message.Message(payload=data)
        assert msg.store_id is None
        assert msg.store_chan_name is None

        # Launch channel processing
        self.start_channels()
        result_generator_msg = self.loop.run_until_complete(chan.handle(msg))
        assert msg.store_id is not None
        assert msg.store_chan_name == chan_name
        msg_store_id = msg.store_id
        for entry in result_generator_msg:
            # Pass the entry in the second chan wo msgstore to verify that the msg attrs
            # are not modified by the second chan
            rslt_msg = self.loop.run_until_complete(chan2_wo_msgstore.handle(entry))
            assert rslt_msg.store_id == msg_store_id
            assert rslt_msg.store_chan_name == chan_name

    def test_memory_message_store_in_fork(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10.25", loop=self.loop, message_store_factory=store_factory)

        n1 = TstNode()
        n2 = TstNode()
        n3 = TstNode()
        n4 = TstNode()

        chan.add(n1, n2)
        fork = chan.fork()
        fork.add(n3)

        self.assertTrue(isinstance(fork.message_store, msgstore.NullMessageStore))

        whe = chan.when(True, message_store_factory=store_factory)
        whe.add(n4)

        self.assertTrue(isinstance(whe.message_store, msgstore.MemoryMessageStore))

    def test_replay_from_memory_message_store(self):
        """ We can store a message in FileMessageStore """

        store_factory = msgstore.MemoryMessageStoreFactory()

        chan = BaseChannel(name="test_channel10.5", loop=self.loop, message_store_factory=store_factory)

        n = TstNode()

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35))

        chan.add(n)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))

        for msg in msg_stored:
            print(msg)

        self.assertEqual(len(msg_stored), 2, "Should be 2 messages in store!")

        self.loop.run_until_complete(chan.replay(msg_stored[0]['id']))

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))
        self.assertEqual(len(msg_stored), 3, "Should be 3 messages in store!")

    def test_file_message_store(self):
        """ We can store a message in FileMessageStore """

        tempdir = tempfile.mkdtemp()
        print(tempdir)

        store_factory = msgstore.FileMessageStoreFactory(path=tempdir)

        chan = BaseChannel(name="test_channel11", loop=self.loop, message_store_factory=store_factory)

        n = TstNode()
        n_error = TstConditionalErrorNode()

        msg = generate_msg(with_context=True)
        msg2 = generate_msg(timestamp=(1982, 11, 27, 12, 35), message_content="message content2")
        msg3 = generate_msg(timestamp=(1982, 11, 28, 12, 35), message_content="message content3")
        msg4 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content="message_content4")

        # This message should be in error
        msg5 = generate_msg(timestamp=(1982, 11, 12, 14, 35),
                            message_content='{"test1": "shall_fail"}')
        msg6 = generate_msg(timestamp=(1982, 11, 28, 14, 35), message_content=ObjectWithoutStr())
        chan.add(n)
        chan.add(n_error)

        # Launch channel processing
        self.start_channels()
        self.loop.run_until_complete(chan.handle(msg))
        self.loop.run_until_complete(chan.handle(msg2))
        self.loop.run_until_complete(chan.handle(msg3))
        self.loop.run_until_complete(chan.handle(msg4))
        with self.assertRaises(TstException):
            # This message should be in error state
            self.loop.run_until_complete(chan.handle(msg5))
        self.loop.run_until_complete(chan.handle(msg6))

        self.assertEqual(self.loop.run_until_complete(chan.message_store.total()),
                         6, "Should be a total of 6 messages in store!")

        msg_stored = list(self.loop.run_until_complete(chan.message_store.search()))
        msg2_id = msg_stored[1]["id"]
        msg_stored1 = list(self.loop.run_until_complete(chan.message_store.search(count=2)))
        self.assertEqual(len(msg_stored1), 2, "Should be 2 results from search!")

        msg_stored2 = list(self.loop.run_until_complete(chan.message_store.search(start_id=msg2_id, count=5)))
        self.assertEqual(len(msg_stored2), 4, "Should be 4 results from search!")

        # All message stored ?
        self.assertEqual(len(msg_stored), 6, "Should be 6 messages in store!")

        for msg in msg_stored:
            print(msg)

        for msg in msg_stored1 + msg_stored2:
            print(msg)

        ids1 = [msg['id'] for msg in msg_stored1 + msg_stored2]
        ids2 = [msg['id'] for msg in msg_stored]

        self.assertEqual(ids1, ids2, "Should be 5 messages in store!")

        # Test processed message
        dict_msg = self.loop.run_until_complete(
            chan.message_store.get('19821128_123500000000_%s' % msg3.uuid))
        self.assertEqual(dict_msg['state'], 'processed', "Message %s should be in processed state!" % msg3)

        # Test failed message
        dict_msg = self.loop.run_until_complete(
            chan.message_store.get('19821112_143500000000_%s' % msg5.uuid))
        self.assertEqual(dict_msg['state'], 'error', "Message %s should be in error state!" % msg5)

        self.assertTrue(os.path.exists("%s/%s/1982/11/28/19821128_123500000000_%s"
                        % (tempdir, chan.name, msg3.uuid)))

        # Test list messages
        msgs = self.loop.run_until_complete(chan.message_store.search(start_id=msg2_id, count=5))
        self.assertEqual(len(msgs), 4, "Failure of listing messages for file msg store")

        # Test list messages with date filters
        msgs = self.loop.run_until_complete(chan.message_store.search(
            start_dt="1982-11-27", end_dt="1982-11-28T13:00:00"))
        self.assertEqual(len(msgs), 2, "Failure of listing messages for file msg store")

        # Test list messages with text filters
        msgs = self.loop.run_until_complete(chan.message_store.search(
            text="sage con"))
        self.assertEqual(len(msgs), 2, "Failure of listing messages for file msg store")

        # Test list messages with regex filters
        msgs = self.loop.run_until_complete(chan.message_store.search(
            rtext="\w+_\w+"))  # noqa: W605
        self.assertEqual(len(msgs), 1, "Failure of listing messages for file msg store")

        # Test view message
        msg_content = self.loop.run_until_complete(chan.message_store.get_msg_content(
            '19821112_143500000000_%s' % msg5.uuid))
        self.assertEqual(msg_content.payload, msg5.payload, "Failure of message %s view!" % msg5)

        # Test preview message
        msg_content = self.loop.run_until_complete(chan.message_store.get_preview_str(
            '19821112_143500000000_%s' % msg5.uuid))
        self.assertEqual(msg_content.payload, msg5.payload[:1000], "Failure of message %s preview!" % msg5)

        self.clean_loop()

        # TODO put in tear_down ?
        shutil.rmtree(tempdir, ignore_errors=True)

    def test_file_message_store_meta(self):
        """Tests for meta in file message store"""
        data_tst_path = Path(__file__).resolve().parent / "data"
        old_meta_tst_path = data_tst_path / "old_meta.meta"
        new_meta_tst_path = data_tst_path / "new_meta.meta"
        with new_meta_tst_path.open("r") as fin:
            new_meta_data = json.load(fin)
        msg_uid = "msgid"
        msg_year = "2024"
        msg_month = "06"
        msg_day = "13"
        msg_date = f"{msg_year}{msg_month}{msg_day}"
        msg_time = "0000"
        msg_id = f"{msg_date}_{msg_time}_{msg_uid}"

        with tempfile.TemporaryDirectory() as tempdir:
            store_factory = msgstore.FileMessageStoreFactory(path=tempdir)
            store = store_factory.get_store(store_id="")
            meta_dst_folder_path = Path(tempdir) / msg_year / msg_month / msg_day
            meta_dst_folder_path.mkdir(parents=True, exist_ok=True)
            meta_dst_path = meta_dst_folder_path / f"{msg_id}.meta"
            shutil.copy(old_meta_tst_path, meta_dst_path)
            # Tests that msgstore could read an old meta file
            msg_state = asyncio.run(store.get_message_state(msg_id))
            self.assertEqual(msg_state, "processed")
            # Test that the old meta file is converted to a new json meta file
            with meta_dst_path.open("r") as fin:
                meta_data = json.load(fin)
            self.assertDictEqual(new_meta_data, meta_data)

    def test_search_meta_filter_sort(self):
        """Retrieve, filter and sort messages based on meta info"""

        results = [
            {'id': str(k), 'message': Message(meta=meta)}  # trimmed version with only what we need
            for k, meta in enumerate((
                {
                    'one': 'one',
                    'two': ['uouii', 'ononn'],
                    'num': ['42'],
                },
                {
                    'one': ['not', 'today', '!'],
                    'two': ['-', 'okiuki', '-'],
                    'num': ['15'],
                },
                {
                    'one': ['yesterday', '?'],
                    'num': ['12', '72'],
                },
                {
                    'one': [],
                    'num': ['notnum.. sad'],
                },
                {
                    'one': [],
                    'num': 37,
                },
            ))
        ]

        # filt/ord by .. should yield .. (indices in list)
        cases = [
            ({'exact_one': 'one'}, [0]),
            ({'text_one': 'day'}, [1, 2]),
            ({'exact_two': 'okiuki'}, [1]),
            ({'rtext_two': '(.).{2}\\1'}, [0, 1]),
            ({'start_num': '17'}, [0, 2, 4]),
            ({'end_num': '17'}, [1, 2]),
            ({'text_one': 'day', 'start_num': '22'}, [2]),

            # specifying a range via start+end, with the message having ['12', '72']:
            # * what you might be expecting:
            #     12  [40   60]  72   -> not in
            # * what it's actually doing:
            #     12  [40   60   72   -> yes above
            #     12   40   60]  72   -> yes below
            #                           `> yes in
            ({'start_num': '40', 'end_num': '60'}, [0, 2]),

            ({'order_by': 'num'}, [2, 1, 4, 0, 3]),
            ({'order_by': '-num'}, [3, 0, 4, 1, 2]),

            # no search param should forward all the results
            ({}, list(range(len(results)))),
        ]

        for search, expect in cases:
            actual = [
                int(item['id'])
                for item in
                msgstore.MessageStore._search_meta_filter_sort(search, results)
            ]
            self.assertListEqual(expect, actual)
