"""The CLI for pypeman.

The script entry point is :func:`pypeman.commands.main`, this is the
function called directly when invoking `$ pypeman` after `$ pip install`
-ing it.

:func:`pypeman.command.amain` is the actuall main function, which is
called when used as a module (eg `$ python -m pypeman.commands`).
"""

from __future__ import annotations

import asyncio
import importlib
from argparse import ArgumentParser
from argparse import Namespace
from logging import getLogger

from .channels import all_channels
from .conf import settings
from .endpoints import all_endpoints
from .graph import load_project
from .plugin_mgr import manager
from .plugins.base import CommandPluginMixin
from .plugins.base import TaskPluginMixin

logger = getLogger(__name__)


def load_project():
    """Helper to load the user project consistently.

    This means:
        * ensure settings are properly loaded
        * import the project module
        * logs; that's all folks
    """
    settings.raise_for_missing()

    logger.debug(f"Loading ({settings.PROJECT_MODULE})...")
    importlib.import_module(settings.PROJECT_MODULE)
    logger.debug("Project loaded successfully.")


async def start(_options: Namespace):
    load_project()

    await asyncio.gather(*(task.task_start() for task in manager.get_plugins(TaskPluginMixin)))

    await asyncio.gather(*(it.start() for it in all_endpoints + all_channels))
    # TODO: check this point, ordering might matter (all endpoints then all channels)

    # TODO/FIXME: after the line above, essentially all error within
    #             this function are inhibited (? wth) wanna investigate

    logger.debug("Everything ready.")
    try:
        while ...:
            await asyncio.sleep(43210)
            logger.debug("Still live and kicking.")
    except KeyboardInterrupt:
        logger.debug("SIGINT!")
    logger.debug("Loop was stopped.")

    await asyncio.gather(*(it.stop() for it in all_endpoints + all_channels))

    await asyncio.gather(*(task.task_stop() for task in manager.get_plugins(TaskPluginMixin)))


async def amain():
    parser = ArgumentParser()
    subpar = parser.add_subparsers(dest="command", required=True)

    # `start` isn't moved to a plugin (lucky little one)
    subpar.add_parser("start").set_defaults(_func=start)

    manager.register_plugins(*settings.PLUGINS)
    # side-note: the 'discovery' of plugins relies solely on
    #            "settings.py" and i dont like "settings.py"
    # also there is a world in which we `parser.parse_known_args` early
    # to collect a list of "--plugin <badibooda>" for example, or even
    # "--no-plugin", etc..
    manager.instantiate_plugins()

    for com in manager.get_plugins(CommandPluginMixin):
        com_parser = subpar.add_parser(com.command_name())
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
    asyncio.run(amain())
