"""Tests for the plugin base classes and the plugin manager."""

import pytest

from pypeman.plugin_mgr import PluginManager
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import CommandPluginMixin
from pypeman.plugins.base import TaskPluginMixin
from pypeman.plugins.graph import GraphPlugin
from pypeman.plugins.plugins import ListPluginsPlugin
from pypeman.plugins.remoteadmin import RemoteAdminPlugin


def test_command_name_derivation():
    class HelloPlugin(BasePlugin, CommandPluginMixin):
        @classmethod
        def command_parse(cls, parser):
            pass

        async def command(self, options):
            pass

    assert HelloPlugin.command_name() == "hello"
    # overridable
    assert RemoteAdminPlugin.command_name() == "shell"


def test_manager_registration_order_and_dedup():
    manager = PluginManager()
    manager.register_plugins(
        "pypeman.plugins.graph.GraphPlugin",
        "pypeman.plugins.plugins.ListPluginsPlugin",
        "pypeman.plugins.graph.GraphPlugin",  # duplicate, ignored
    )
    assert manager._registered_classes == [GraphPlugin, ListPluginsPlugin]


def test_manager_rejects_non_plugin():
    manager = PluginManager()
    with pytest.raises(AssertionError):
        manager.register_plugins("pypeman.plugin_mgr.PluginManager")


def test_manager_instantiate_and_get_plugins():
    manager = PluginManager()
    manager.register_plugins(
        "pypeman.plugins.graph.GraphPlugin",
        "pypeman.plugins.plugins.ListPluginsPlugin",
    )
    manager.instantiate_plugins()

    commands = list(manager.get_plugins(CommandPluginMixin))
    assert [type(it) for it in commands] == [GraphPlugin, ListPluginsPlugin]
    assert list(manager.get_plugins(TaskPluginMixin)) == []
    assert [type(it) for it in manager.get_all_plugins()] == [GraphPlugin, ListPluginsPlugin]


def test_manager_lifecycle_assertions():
    manager = PluginManager()
    with pytest.raises(AssertionError):
        list(manager.get_all_plugins())  # not instantiated yet

    manager.instantiate_plugins()
    with pytest.raises(AssertionError):
        manager.instantiate_plugins()  # twice
    with pytest.raises(AssertionError):
        manager.register_plugins("pypeman.plugins.graph.GraphPlugin")  # too late
