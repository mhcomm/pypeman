"""Tests for the remoteadmin plugin (ws RPC validation + plugin config)."""

import logging

import pytest

from pypeman.conf import settings
from pypeman.plugins.base import webapp_bundle
from pypeman.plugins.remoteadmin import methods
from pypeman.plugins.remoteadmin.plugin import RemoteAdminPlugin
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


@pytest.fixture
def plugin_env():
    webapp_bundle._reset()
    yield
    webapp_bundle._reset()


def test_plugin_default_prefix(plugin_env):
    plugin = RemoteAdminPlugin()
    # no prefix by default: served at the root of the shared app
    assert plugin.webapp_prefix() == ""
    # instantiating registered the plugin into the shared bundle
    assert plugin in webapp_bundle._members


def test_plugin_prefix_from_settings(plugin_env, monkeypatch):
    monkeypatch.setitem(settings.__dict__, "REMOTE_ADMIN_CONFIG", {"url": "/admin"})
    assert RemoteAdminPlugin().webapp_prefix() == "/admin"


def test_plugin_prefix_normalized(plugin_env, monkeypatch):
    # a legacy '/' (or a trailing slash) still means the root
    monkeypatch.setitem(settings.__dict__, "REMOTE_ADMIN_CONFIG", {"url": "/"})
    assert RemoteAdminPlugin().webapp_prefix() == ""


def test_plugin_warns_on_ignored_host_port(plugin_env, monkeypatch, caplog):
    monkeypatch.setitem(
        settings.__dict__, "REMOTE_ADMIN_CONFIG", {"url": "/admin", "host": "localhost"}
    )
    with caplog.at_level(logging.WARNING):
        RemoteAdminPlugin()
    assert any("host/port are ignored" in rec.message for rec in caplog.records)


def test_plugin_deprecated_settings(plugin_env, monkeypatch, caplog):
    monkeypatch.setitem(
        settings.__dict__,
        "REMOTE_ADMIN_WEBSOCKET_CONFIG",
        {"host": "localhost", "port": "8091", "url": "/old"},
    )
    with caplog.at_level(logging.WARNING):
        plugin = RemoteAdminPlugin()
    assert plugin.webapp_prefix() == "/old"
    assert any("deprecated" in rec.message for rec in caplog.records)


def test_plugin_no_deprecation_warning_by_default(plugin_env, caplog):
    # REMOTE_ADMIN_WEBSOCKET_CONFIG is no longer part of the defaults,
    # so the deprecation must not fire unless the user defines it
    assert not hasattr(settings, "REMOTE_ADMIN_WEBSOCKET_CONFIG")
    with caplog.at_level(logging.WARNING):
        RemoteAdminPlugin()
    assert not any("deprecated" in rec.message for rec in caplog.records)
