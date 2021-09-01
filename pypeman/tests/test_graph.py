import asyncio
from unittest import mock

import pytest

from pypeman import channels
from pypeman import nodes
from pypeman.errors import PypemanError
from pypeman.graph import mk_graph
from pypeman.graph import wait_for_loop
from pypeman.tests.pytest_helpers import clear_graph  # noqa: F401


class FakeChannel:
    def __init__(self, loop=None):
        if loop:
            self.loop

    def __repr__(self):
        return "FakeChannel(loop=" + repr(getattr(self, "loop", "not set")) + ")"


def mk_fake_all_channels(t=0):
    """
    helper to moke pypeman.channels.all_channels

    :param t: time after which channel.loop should be
              set
    """
    channel = FakeChannel(loop=None)
    all_channels = [channel]
    if t < 0:
        return all_channels
    if t == 0:
        channel.loop = True
        return all_channels
    # TODO: start thread to set channel.loop from 0 to 1
    return all_channels


@mock.patch('pypeman.channels.all_channels', mk_fake_all_channels(t=-1))
def test_have_no_loop():
    with pytest.raises(PypemanError):
        wait_for_loop(tmax=0.1)


@mock.patch('pypeman.channels.all_channels', mk_fake_all_channels(t=0))
def test_have_loop():
    loop = wait_for_loop(tmax=0.1)
    print("loop =", loop)
    assert loop, "loop should be set, but is %s" % loop


@pytest.mark.usefixtures("clear_graph")
def test_show_graph():
    """ is a graph generated """
    loop = asyncio.new_event_loop()
    print("loop =", loop)
    names = (f"n{cnt}" for cnt in range(10))
    chan1 = channels.BaseChannel(name=next(names), loop=loop)
    chan1.add(nodes.BaseNode(name=next(names)))
    chan1.add(nodes.BaseNode(name=next(names)))
    subchan = chan1.when(lambda v: True, name=next(names))
    subchan.add(nodes.BaseNode(name=next(names)))
    rslt = list(mk_graph())
    as_str = "\n".join(rslt)
    print("RSLT:", as_str)
    print(repr(as_str))
    assert len(rslt) == 8
    assert as_str == (
        "BaseChannel\n|-n1\n|-n2\n|?\\ (n0.n3)\n|"
        "  |-n4\n|  -> Out\n|-> out\n")
