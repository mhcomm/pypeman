"""Provies :class:`PrintSettingsPlugin`."""

from __future__ import annotations

from argparse import ArgumentParser
from argparse import Namespace

from ..conf import settings
from .base import BasePlugin
from .base import CommandPluginMixin


class PrintSettingsPlugin(BasePlugin, CommandPluginMixin):
    """Provides the `printsettings` command."""

    # legacy-ass comment XXX
    # TODO: This command could be enhanced further to be more simniliar to
    #       django_extensions' print_settings

    @classmethod
    def command_parse(cls, parser: ArgumentParser):
        parser.add_argument(
            "--sort",
            choices={"none", "name", "value"},
            default="name",
            help="specify order to sort in; 'none' will sort by declaration order",
        )

    async def command(self, options: Namespace):
        ls = filter(lambda p: "A" <= p[0][0] <= "Z", vars(settings).items())

        if "none" != options.sort:
            ls = sorted(ls, key=(lambda p: p[0]) if "name" == options.sort else (lambda p: str(p[1])))

        for name, value in ls:
            print(name, "=", value)

        # afterward, after listing the effective settings,
        # notify the user (and crash) if the module couldn't be loaded
        settings.raise_for_missing()
