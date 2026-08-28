"""
tests for the logging revamp: LogContextFilter, per-channel logger naming
and the log level policy
"""

import asyncio
import logging

from contextlib import contextmanager

import pytest

from pypeman import nodes
from pypeman.channels import BaseChannel
from pypeman.exceptions import Dropped, Rejected
from pypeman.helpers.logging import CHANNEL_CTXVAR
from pypeman.helpers.logging import DebugLogHandler
from pypeman.helpers.logging import LogContextFilter
from pypeman.helpers.logging import MSG_CTXVAR
from pypeman.tests.common import generate_msg
from pypeman.tests.common import TstNode
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401


class DropNode(nodes.BaseNode):
    def process(self, msg):
        raise Dropped()


class RejectNode(nodes.BaseNode):
    def process(self, msg):
        raise Rejected()


def make_record(logger_name="test"):
    return logging.LogRecord(logger_name, logging.INFO, __file__, 1, "hello", None, None)


@pytest.fixture
def loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@contextmanager
def capture_records(chan):
    """Collect at DEBUG every record logged through a channel's logger.

    A handler of our own rather than pytest's `caplog`: the lazy settings
    loading calls `logging.config.dictConfig`, which drops the handlers
    caplog installs on the root logger.
    """
    handler = DebugLogHandler()
    previous_level = chan.logger.level
    chan.logger.addHandler(handler)
    chan.logger.setLevel(logging.DEBUG)
    try:
        yield handler.log_trace
    finally:
        chan.logger.setLevel(previous_level)
        chan.logger.removeHandler(handler)


def test_context_filter():
    flt = LogContextFilter()
    record = make_record()

    # No message being processed: empty msg_id and channel
    assert flt.filter(record) is True
    assert record.msg_id == ""
    assert record.channel == ""

    msg = generate_msg()
    token = MSG_CTXVAR.set(msg)
    try:
        record = make_record()
        assert flt.filter(record) is True
        assert record.msg_id == "[%s] " % msg.uuid
        assert msg.short_uuid == msg.uuid[:8]
    finally:
        MSG_CTXVAR.reset(token)

    record = make_record()
    flt.filter(record)
    assert record.msg_id == ""


@pytest.mark.usefixtures("clear_graph")
def test_context_filter_channel(loop):
    flt = LogContextFilter()
    chan = BaseChannel(name="ctx_chan", loop=loop)
    token = CHANNEL_CTXVAR.set(chan)
    try:
        # Foreign logger (e.g. project code): channel name injected
        record = make_record("myapp.mynodes")
        flt.filter(record)
        assert record.channel == "(ctx_chan) "

        # Channel's own logger already carries the name: nothing injected
        record = make_record(chan.logger.name)
        flt.filter(record)
        assert record.channel == ""
    finally:
        CHANNEL_CTXVAR.reset(token)


class AppLoggerNode(nodes.BaseNode):
    """ Mimics project code logging through its own module logger. """
    app_logger = logging.getLogger("myapp.mynodes")

    def process(self, msg):
        self.app_logger.info("app line")
        return msg


@pytest.mark.usefixtures("clear_graph")
def test_context_in_records_during_processing(loop):
    chan = BaseChannel(name="logging_chan", loop=loop)
    chan.add(AppLoggerNode(name="app_node"))

    handler = DebugLogHandler()
    handler.addFilter(LogContextFilter())
    chan.logger.addHandler(handler)
    chan.logger.setLevel(logging.DEBUG)
    app_logger = AppLoggerNode.app_logger
    app_logger.addHandler(handler)
    app_logger.setLevel(logging.DEBUG)
    try:
        loop.run_until_complete(chan.start())
        msg = generate_msg()
        loop.run_until_complete(chan.handle(msg))
    finally:
        chan.logger.removeHandler(handler)
        app_logger.removeHandler(handler)

    records = handler.log_trace
    started = [rec for rec in records if rec.getMessage() == "channel logging_chan started"]
    assert started and started[0].msg_id == "" and started[0].channel == ""

    processing_texts = (
        "channel logging_chan handling new msg %s" % msg.short_uuid,
        "msg %s processed" % msg.short_uuid,
    )
    processing = [rec for rec in records if rec.getMessage() in processing_texts]
    assert len(processing) == 2
    for rec in processing:
        assert rec.msg_id == "[%s] " % msg.uuid
        # channel's own logger: no redundant channel injection
        assert rec.channel == ""

    # Project code logging through its own logger gets both attributes
    app_lines = [rec for rec in records if rec.getMessage() == "app line"]
    assert len(app_lines) == 1
    assert app_lines[0].msg_id == "[%s] " % msg.uuid
    assert app_lines[0].channel == "(logging_chan) "


@pytest.mark.usefixtures("clear_graph")
def test_channel_logger_short_name(loop):
    chan = BaseChannel(name="parent_chan", loop=loop)
    sub = chan.fork(name="sub_chan")

    assert chan.logger.name == "pypeman.channels.parent_chan"
    assert sub.name == "parent_chan.sub_chan"
    assert sub.logger.name == "pypeman.channels.sub_chan"

    # Short names must be unique, even between top level channels and subchannels
    with pytest.raises(NameError):
        BaseChannel(name="sub_chan", loop=loop)


@pytest.mark.usefixtures("clear_graph")
def test_level_policy(loop):
    chan = BaseChannel(name="levels_chan", loop=loop)
    chan.add(TstNode(name="tst_node"))
    loop.run_until_complete(chan.start())

    # Success: one receipt + one outcome line at INFO, node enter/exit at DEBUG
    msg = generate_msg()
    with capture_records(chan) as records:
        loop.run_until_complete(chan.handle(msg))
    infos = [rec.getMessage() for rec in records if rec.levelno == logging.INFO]
    assert infos == [
        "channel levels_chan handling new msg %s" % msg.short_uuid,
        "msg %s processed" % msg.short_uuid,
    ]
    debugs = [rec.getMessage() for rec in records if rec.levelno == logging.DEBUG]
    assert any(text.startswith("msg %s infos:" % msg.short_uuid) for text in debugs)
    assert "node tst_node: enter, msg %s (payload %s)" % (
        msg.short_uuid, type(msg.payload).__name__) in debugs
    assert any(text.startswith("node tst_node: exit after") for text in debugs)

    # Drop: INFO outcome
    drop_chan = BaseChannel(name="drop_chan", loop=loop)
    drop_chan.add(DropNode(name="drop_node"))
    loop.run_until_complete(drop_chan.start())
    msg = generate_msg()
    with capture_records(drop_chan) as records:
        loop.run_until_complete(drop_chan.handle(msg))
    dropped = [rec for rec in records
               if rec.getMessage().startswith("msg %s dropped" % msg.short_uuid)]
    assert dropped and dropped[0].levelno == logging.INFO

    # Reject: WARNING outcome
    reject_chan = BaseChannel(name="reject_chan", loop=loop)
    reject_chan.add(RejectNode(name="reject_node"))
    loop.run_until_complete(reject_chan.start())
    msg = generate_msg()
    with capture_records(reject_chan) as records:
        with pytest.raises(Rejected):
            loop.run_until_complete(reject_chan.handle(msg))
    rejected = [rec for rec in records
                if rec.getMessage().startswith("msg %s rejected" % msg.short_uuid)]
    assert rejected and rejected[0].levelno == logging.WARNING

    # Failure: single ERROR without traceback text
    fail_chan = BaseChannel(name="fail_chan", loop=loop)
    fail_chan.add(nodes.RaiseError(name="fail_node"))
    loop.run_until_complete(fail_chan.start())
    with capture_records(fail_chan) as records:
        with pytest.raises(Exception):
            loop.run_until_complete(fail_chan.handle(generate_msg()))
    errors = [rec for rec in records if rec.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is None
