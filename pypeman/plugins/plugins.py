"""Provides :class:`ListPluginsPlugin`."""

from __future__ import annotations

from argparse import ArgumentParser
from argparse import Namespace

from pypeman.conf import settings
from pypeman.plugin_mgr import manager
from pypeman.plugins.base import BasePlugin
from pypeman.plugins.base import CommandPluginMixin


class ListPluginsPlugin(BasePlugin, CommandPluginMixin):
    """Provides this very `listplugins` command."""

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        pass  # no option nor argument

    async def command(self, options: Namespace):
        for it in manager.get_all_plugins():
            cls = type(it)
            doc = cls.__doc__ or "(this plugin has no documentation)"
            print(f"{cls.__module__} {cls.__name__}:")
            print("   ", doc.strip())
            print()

        # afterward, after listing the effective plugins,
        # notify the user (and crash) if the module couldn't be loaded
        settings.raise_for_missing()
