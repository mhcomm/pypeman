"""
tests for pypeman.contrib.ctx
"""

import asyncio

import pytest

from pypeman.contrib.ctx import CombineCtx
from pypeman.nodes import NodeException
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401

from pypeman.tests.common import generate_msg
# TODO: might refactor to another file?
from pypeman.tests.test_nodes import FakeChannel


def mk_msgs_w_ctx(*ctx_ids):
    """
    helper to create a msg with a few contexts
    """
    msg = generate_msg(
        message_content="test",
        message_meta=dict(entry1="meta1"),
        )
    ctx_msgs = []
    for ctx_id in ctx_ids:
        meta = dict(entry1="meta_%s" % ctx_id)
        ctx_msg = generate_msg(
            message_content={"val_%s" % ctx_id: "data_%s" % ctx_id},
            message_meta=meta,
            )
        msg.add_context(ctx_id, ctx_msg)
        ctx_msgs.append(ctx_msg)

    return msg, ctx_msgs


@pytest.mark.usefixtures("clear_graph")
def test_combine_ctx_not_two_names(event_loop):
    with pytest.raises(NodeException):
        CombineCtx([], name="ctx1")
    with pytest.raises(NodeException):
        CombineCtx(["a"], name="ctx2")


@pytest.mark.usefixtures("clear_graph")
def test_combine_ctx_2_names(event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    # nut == Node Under Test
    nut = CombineCtx(["a", "b"], name="ctx1")
    nut.channel = FakeChannel(loop)
    msg, ctx_msgs = mk_msgs_w_ctx("a", "b")

    rslt = loop.run_until_complete(nut.handle(msg))
    assert rslt.payload["a"] == ctx_msgs[0].payload
    assert rslt.payload["b"] == ctx_msgs[1].payload
    assert rslt.meta == ctx_msgs[0].meta


@pytest.mark.usefixtures("clear_graph")
def test_combine_ctx_2_names_w_meta(event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    # nut == Node Under Test
    nut = CombineCtx(["a", "b"], meta_from="b", name="ctx1")
    nut.channel = FakeChannel(loop)
    msg, ctx_msgs = mk_msgs_w_ctx("a", "b")

    rslt = loop.run_until_complete(nut.handle(msg))
    assert rslt.payload["a"] == ctx_msgs[0].payload
    assert rslt.payload["b"] == ctx_msgs[1].payload
    assert rslt.meta == ctx_msgs[1].meta


@pytest.mark.usefixtures("clear_graph")
def test_combine_ctx_2_names_flat(event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    # nut == Node Under Test
    nut = CombineCtx(["a", "b"], name="ctx1", flatten=True)
    nut.channel = FakeChannel(loop)
    msg, ctx_msgs = mk_msgs_w_ctx("a", "b")

    rslt = loop.run_until_complete(nut.handle(msg))
    exp_payload = dict(ctx_msgs[0].payload)
    exp_payload.update(ctx_msgs[1].payload)
    assert rslt.payload == exp_payload
    assert rslt.meta == ctx_msgs[0].meta
