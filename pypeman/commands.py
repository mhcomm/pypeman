"""The CLI for pypeman.

The script entry point is :func:`pypeman.commands.main`, this is the
function called directly when invoking `$ pypeman` after `$ pip install`
-ing it.

:func:`pypeman.command.amain` is the actuall main function, which is
called when used as a module (eg `$ python -m pypeman.commands`).
"""

from __future__ import annotations

import asyncio
from argparse import ArgumentParser
from argparse import Namespace
from logging import getLogger

from pypeman import __version__
from pypeman.channels import all_channels
from pypeman.conf import settings
from pypeman.endpoints import all_endpoints
from pypeman.plugin_mgr import manager
from pypeman.plugins.base import CommandPluginMixin
from pypeman.plugins.base import TaskPluginMixin
from pypeman.project import load_project

__all__ = ("load_project", "start", "amain", "main")

logger = getLogger(__name__)


async def start(_options: Namespace):
    load_project()

    await asyncio.gather(*(task.task_start() for task in manager.get_plugins(TaskPluginMixin)))

    await asyncio.gather(*(it.start() for it in all_endpoints + all_channels))
    # TODO: check this point, ordering might matter (all endpoints then all channels)

    logger.debug("Everything ready.")
    # TODO: graceful shutdown (loop signal handlers for SIGINT/SIGTERM
    #       + an event instead of this idle loop) so the stop sequence
    #       below actually runs; deliberately left for a dedicated rework
    while ...:
        await asyncio.sleep(43210)
        logger.debug("Still live and kicking.")

    await asyncio.gather(*(it.stop() for it in all_endpoints + all_channels))

    await asyncio.gather(*(task.task_stop() for task in manager.get_plugins(TaskPluginMixin)))


async def amain():
    parser = ArgumentParser(prog="pypeman")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subpar = parser.add_subparsers(dest="command", required=True)

    # `start` is the only command not provided through a plugin
    subpar.add_parser("start", help="start the pypeman project").set_defaults(_func=start)

    manager.register_plugins(*settings.PLUGINS)
    manager.instantiate_plugins()

    for com in manager.get_plugins(CommandPluginMixin):
        doc = (type(com).__doc__ or "").strip().splitlines()
        help_line = doc[0] if doc else None
        com_parser = subpar.add_parser(com.command_name(), help=help_line, description=help_line)
        com.command_parse(com_parser)
        com_parser.set_defaults(_func=com.command)

    options = parser.parse_args()
    await options._func(options)


def main():
    """Entry point for the console script `$ pypeman`.

    This intermediary function is needed for 2 reasons:
        1. setuptool's console_scripts' entry_points cannot be async;
        2. sys.path initialization [differs](https://docs.python.org/3/library/sys.html#sys.path).

    When invoked as a module (`$ python -m pypeman.commands`), it acts
    as expected (the current working directory is part of the path).
    However Python dictates that when invoking a script (`$ pypeman`)
    **the script's directory** is swapped in instead!

    To palliate for this, we prepend the user's working directory
    manually. This is only necessary when invoked as an installed
    console script.
    """
    import os
    import sys

    sys.path.insert(0, os.getcwd())
    asyncio.run(amain())


if "__main__" == __name__:
    # running the file as a script has the same sys.path quirk as the
    # console script (the script's directory replaces the CWD), so go
    # through main() for its sys.path fixup
    main()
