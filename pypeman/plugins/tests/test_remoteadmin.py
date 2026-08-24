"""Tests for the remoteadmin plugin (ws RPC validation + plugin config)."""

import pytest

from pypeman.plugins.remoteadmin import methods
from pypeman.plugins.remoteadmin.urls import _check_params


@pytest.mark.parametrize(
    "rfn, params, expect_valid",
    [
        # no parameter at all
        (methods.list_channels, {}, True),
        (methods.list_channels, {"nope": "1"}, False),
        # one required kw-only parameter
        (methods.start_channel, {"channelname": "chan"}, True),
        (methods.start_channel, {}, False),
        (methods.start_channel, {"channelname": "chan", "extra": "1"}, False),
        # required kw-only parameter + **search_kwargs
        (methods.list_msgs, {"channelname": "chan"}, True),
        (methods.list_msgs, {"channelname": "chan", "count": "10", "order_by": "-timestamp"}, True),
        (methods.list_msgs, {"count": "10"}, False),
        # params must be a JSON object (dict)
        (methods.start_channel, ["chan"], False),
    ],
)
def test_check_params(rfn, params, expect_valid):
    valid, _expects = _check_params(rfn, params)
    assert valid is expect_valid
