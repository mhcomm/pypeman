"""Tests for the message meta extender plugin."""

import pytest

from pypeman import events
from pypeman import msgstore
from pypeman.channels import BaseChannel
from pypeman.nodes import BaseNode
from pypeman.nodes import Sleep
from pypeman.plugins.msgmetaextender import MsgMetaExtenderPlugin
from pypeman.plugins.msgmetaextender import payload_size
from pypeman.tests.common import ExceptNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401 (fixture)


@pytest.fixture
async def plugin():
    """A started plugin, stopped (hence unsubscribed) afterwards."""
    plugin = MsgMetaExtenderPlugin()
    await plugin.task_start()
    yield plugin
    await plugin.task_stop()


def test_payload_size():
    assert payload_size(b"12345") == 5
    assert payload_size(bytearray(b"123")) == 3
    assert payload_size(memoryview(b"1234")) == 4
    assert payload_size("héhé") == 6  # utf-8 byte length, not char count
    assert payload_size("é" * 70000) == 140000  # non-ascii, several chunks
    assert payload_size({"not": "measurable"}) is None
    assert payload_size(None) is None


@pytest.mark.usefixtures("clear_graph")
async def test_metas_are_stored_only(plugin):
    chan = BaseChannel(
        name="msgmetaext_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    chan.add(Sleep(name="msgmetaext_sleep", duration=0.05))
    await chan.start()

    msg = generate_msg(message_content="héhé")
    msg.add_context("saved", generate_msg(message_content=b"1234567890"))
    result = await chan.handle(msg)

    # the metas go to the store entry and nowhere else
    assert "process_time" not in result.meta
    assert "input_size" not in msg.meta

    stored_meta = await chan.message_store.get_message_meta_infos(msg.store_id)
    assert stored_meta["process_time"] >= 0.05
    assert stored_meta["input_size"] == 6
    assert stored_meta["input_type"] == "str"
    assert stored_meta["content_type"] == "application/text"
    assert stored_meta["output_size"] == 6  # Sleep returns the message untouched
    assert stored_meta["output_type"] == "str"
    assert stored_meta["ctx_size"] == 10
    assert not plugin._inflight  # no leak


@pytest.mark.usefixtures("clear_graph")
async def test_unmeasurable_payload_has_no_size_metas(plugin):
    chan = BaseChannel(
        name="msgmetaext_obj_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    await chan.start()

    msg = generate_msg(message_content={"a": 1})
    await chan.handle(msg)

    stored_meta = await chan.message_store.get_message_meta_infos(msg.store_id)
    assert "input_size" not in stored_meta
    assert "output_size" not in stored_meta
    assert stored_meta["input_type"] == "dict"
    assert stored_meta["output_type"] == "dict"
    assert stored_meta["content_type"] == "application/text"
    assert stored_meta["ctx_size"] == 0


@pytest.mark.usefixtures("clear_graph")
async def test_error_path_stores_input_metas_only(plugin):
    chan = BaseChannel(
        name="msgmetaext_exc_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    chan.add(ExceptNode(name="msgmetaext_exc_node"))
    await chan.start()

    msg = generate_msg(message_content=b"123")
    with pytest.raises(TstException):
        await chan.handle(msg)

    stored_meta = await chan.message_store.get_message_meta_infos(msg.store_id)
    assert stored_meta["input_size"] == 3
    assert stored_meta["process_time"] > 0
    assert "output_size" not in stored_meta
    assert "output_type" not in stored_meta
    assert not plugin._inflight  # entry dropped, no leak


@pytest.mark.usefixtures("clear_graph")
async def test_ctx_size_on_error_path_counts_entry_contexts_only(plugin):
    """The contexts added by the nodes are unreachable when handling raised."""
    class CtxAddingExceptNode(BaseNode):
        def process(self, msg):
            msg.add_context("added_on_the_way", generate_msg(message_content=b"0" * 100))
            raise TstException()

    chan = BaseChannel(
        name="msgmetaext_ctx_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    chan.add(CtxAddingExceptNode(name="msgmetaext_ctx_node"))
    await chan.start()

    msg = generate_msg(message_content=b"123")
    msg.add_context("at_entry", generate_msg(message_content=b"1234567890"))
    with pytest.raises(TstException):
        await chan.handle(msg)

    stored_meta = await chan.message_store.get_message_meta_infos(msg.store_id)
    assert stored_meta["ctx_size"] == 10  # the 100 bytes added by the node are lost


@pytest.mark.usefixtures("clear_graph")
async def test_forked_subchannel_store_meta_is_complete(plugin):
    """A fork stores before the events fire, but its metas land all the same."""
    chan = BaseChannel(
        name="msgmetaext_fork_chan", wait_subchans=True,
        message_store_factory=msgstore.MemoryMessageStoreFactory())
    fork = chan.fork(
        name="msgmetaext_fork_sub",
        message_store_factory=msgstore.MemoryMessageStoreFactory())
    fork.add(Sleep(name="msgmetaext_fork_sleep", duration=0.05))
    await chan.start()
    await fork.start()

    msg = generate_msg(message_content="héhé")
    await chan.handle(msg)

    stored_meta = await fork.message_store.get_message_meta_infos(msg.uuid)
    assert stored_meta["input_size"] == 6
    assert stored_meta["input_type"] == "str"
    assert stored_meta["content_type"] == "application/text"
    assert stored_meta["process_time"] >= 0.05
    assert not plugin._inflight  # no leak


async def test_task_stop_unsubscribes():
    plugin = MsgMetaExtenderPlugin()
    await plugin.task_start()
    assert plugin._on_start in events.msg_processing_start.handlers
    assert plugin._on_end in events.msg_processing_end.handlers
    await plugin.task_stop()
    assert plugin._on_start not in events.msg_processing_start.handlers
    assert plugin._on_end not in events.msg_processing_end.handlers
