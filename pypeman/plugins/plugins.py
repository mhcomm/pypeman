from argparse import ArgumentParser
from argparse import Namespace

from .base import BasePlugin
from .base import CommandPluginMixin
from ..conf import settings
from ..plugin_mgr import manager


class ListPluginsPlugin(BasePlugin, CommandPluginMixin):
    """Provides this very `listplugins` command."""

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument(
            "--dot",
            action="store_true",
            help="generate output in graphviz 'dot' format",
        )

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
