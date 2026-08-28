"""
tests for pypeman.events and the channel message processing events
"""

import logging

import pytest

from pypeman import channels
from pypeman import events
from pypeman import msgstore
from pypeman.channels import BaseChannel
from pypeman.events import Event
from pypeman.nodes import BaseNode
from pypeman.tests.common import ExceptNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstException
from pypeman.tests.common import TstNode
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401


class TagReaderNode(BaseNode):
    """ Report what the start event handlers left in the meta """
    def process(self, msg):
        msg.meta["seen_by_node"] = msg.meta.get("tag")
        return msg


def mk_channel(name):
    """ helper creating a started channel keeping its messages """
    return BaseChannel(name=name, message_store_factory=msgstore.MemoryMessageStoreFactory())


@pytest.fixture
def recorded_msg_events():
    """ Record the message processing events, unsubscribing afterwards """
    calls = []

    async def on_start(channel, msg):
        calls.append(("start", channel, msg))

    def on_end(channel, msg, result, exc):  # sync handlers are supported too
        calls.append(("end", channel, msg, result, exc))

    events.msg_processing_start.add_handler(on_start)
    events.msg_processing_end.add_handler(on_end)
    yield calls
    events.msg_processing_start.remove_handler(on_start)
    events.msg_processing_end.remove_handler(on_end)


async def test_fire_safely_runs_every_handler(caplog):
    event = Event("tst_event")
    seen = []
    event.add_handler(lambda: seen.append("sync"))

    @event.receiver
    async def broken():
        raise TstException("broken handler")

    with caplog.at_level(logging.ERROR):
        await event.fire_safely()

    assert seen == ["sync"]
    assert "tst_event" in caplog.text
    # ... whereas a plain fire lets the handler error through
    with pytest.raises(TstException):
        await event.fire()


async def test_handler_may_unsubscribe_while_firing():
    event = Event("tst_event")
    seen = []

    def once():
        seen.append(1)
        event.remove_handler(once)

    event.add_handler(once)
    await event.fire_safely()
    await event.fire_safely()

    assert seen == [1]


@pytest.mark.usefixtures("clear_graph")
async def test_channel_fires_start_and_end(recorded_msg_events):
    chan = mk_channel("evt_chan")
    chan.add(TstNode(name="evt_node"))
    await chan.start()

    msg = generate_msg()
    result = await chan.handle(msg)

    start, end = recorded_msg_events
    assert start == ("start", chan, msg)
    assert end == ("end", chan, msg, result, None)


@pytest.mark.usefixtures("clear_graph")
async def test_start_handler_may_enrich_the_message():
    async def tag(channel, msg):
        msg.meta["tag"] = "tagged"

    events.msg_processing_start.add_handler(tag)
    try:
        chan = mk_channel("evt_tag_chan")
        chan.add(TagReaderNode(name="evt_tag_node"))
        await chan.start()

        msg = generate_msg()
        result = await chan.handle(msg)

        # the nodes see it ...
        assert result.meta["seen_by_node"] == "tagged"
        # ... and so does the message store, stamped before the copy
        stored = await chan.message_store.get(msg.store_id)
        assert stored["message"].meta["tag"] == "tagged"
    finally:
        events.msg_processing_start.remove_handler(tag)


@pytest.mark.usefixtures("clear_graph")
async def test_end_event_reports_the_exception(recorded_msg_events):
    chan = mk_channel("evt_exc_chan")
    chan.add(ExceptNode(name="evt_exc_node"))
    await chan.start()

    msg = generate_msg()
    with pytest.raises(TstException):
        await chan.handle(msg)

    _, end = recorded_msg_events
    assert end[3] is None
    assert isinstance(end[4], TstException)


@pytest.mark.usefixtures("clear_graph")
async def test_events_are_fired_once_per_channel(recorded_msg_events):
    chan = mk_channel("evt_parent_chan")
    cond = chan.when(True, name="evt_cond_chan")
    cond.add(TstNode(name="evt_cond_node"))
    for channel in channels.all_channels:
        await channel.start()

    await chan.handle(generate_msg())

    assert [(call[0], call[1]) for call in recorded_msg_events] == [
        ("start", chan),
        ("start", cond),
        ("end", cond),
        ("end", chan),
    ]


@pytest.mark.usefixtures("clear_graph")
async def test_broken_handler_does_not_break_processing(caplog):
    async def broken(**kwargs):
        raise TstException("broken handler")

    events.msg_processing_start.add_handler(broken)
    events.msg_processing_end.add_handler(broken)
    try:
        chan = mk_channel("evt_broken_chan")
        chan.add(TstNode(name="evt_broken_node"))
        await chan.start()

        msg = generate_msg()
        with caplog.at_level(logging.ERROR):
            result = await chan.handle(msg)

        assert result.payload == msg.payload
        assert "msg_processing_start" in caplog.text
        assert "msg_processing_end" in caplog.text
    finally:
        events.msg_processing_start.remove_handler(broken)
        events.msg_processing_end.remove_handler(broken)
