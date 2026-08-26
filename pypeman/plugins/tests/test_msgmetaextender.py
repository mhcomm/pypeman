"""Tests for the message processing time plugin."""

import pytest

from pypeman import events
from pypeman import msgstore
from pypeman.channels import BaseChannel
from pypeman.nodes import Sleep
from pypeman.plugins.msgmetaextender import MsgMetaExtenderPlugin
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


@pytest.mark.usefixtures("clear_graph")
async def test_process_time_is_tagged_everywhere(plugin):
    chan = BaseChannel(
        name="msgmetaext_chan", message_store_factory=msgstore.MemoryMessageStoreFactory())
    chan.add(Sleep(name="msgmetaext_sleep", duration=0.05))
    await chan.start()

    msg = generate_msg()
    result = await chan.handle(msg)

    process_time = msg.meta["process_time"]
    assert process_time >= 0.05
    assert result.meta["process_time"] == process_time
    stored_meta = await chan.message_store.get_message_meta_infos(msg.store_id)
    assert stored_meta["process_time"] == process_time
    assert not plugin._entry_times  # no leak


@pytest.mark.usefixtures("clear_graph")
async def test_entry_time_is_dropped_on_error(plugin):
    chan = BaseChannel(name="msgmetaext_exc_chan")
    chan.add(ExceptNode(name="msgmetaext_exc_node"))
    await chan.start()

    with pytest.raises(TstException):
        await chan.handle(generate_msg())

    assert not plugin._entry_times


async def test_task_stop_unsubscribes():
    plugin = MsgMetaExtenderPlugin()
    await plugin.task_start()
    assert plugin._on_start in events.msg_processing_start.handlers
    assert plugin._on_end in events.msg_processing_end.handlers
    await plugin.task_stop()
    assert plugin._on_start not in events.msg_processing_start.handlers
    assert plugin._on_end not in events.msg_processing_end.handlers
