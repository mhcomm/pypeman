from unittest import mock

import pytest

from pypeman.errors import PypemanError
from pypeman.graph import wait_for_loop


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
