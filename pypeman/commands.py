import asyncio
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


async def start(_options: Namespace):
    load_project()

    await asyncio.gather(*(task.task_start() for task in manager.get_plugins(TaskPluginMixin)))

    await asyncio.gather(*(it.start() for it in all_endpoints + all_channels))
    # TODO: check this point, ordering might matter (all endpoints then all channels)

    ...  # signal handling

    try:
        # hold on, can you nest `run_forever`s? (asyncio.run already is within a `run_forever`)
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logger.debug("SIGINT!")
    logger.debug("Loop was stopped.")

    await asyncio.gather(*(it.stop() for it in all_endpoints + all_channels))

    await asyncio.gather(*(task.task_stop() for task in manager.get_plugins(TaskPluginMixin)))


async def amain():
    parser = ArgumentParser()
    subpar = parser.add_subparsers(dest="command", required=True)

    # `start` isn't moved to a plugin (lucky little one)
    # "lost" (unused and untested) functionalities:
    # - "--reload" (at best it could be a plugin, but personally i'd rather
    #               rely on an external tool like `nodemon` or `watchdog`)
    # - "--debug-asyncio" just use $PYTHONASYNCIODEBUG like a normal person
    # - "--daemon" same idea as "--reload", pypeman could benefit with
    #              actually being what it sais ie 'minimalistic and whatnotic'
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
    # kept first class:
    # - start
    # moved to plugins:
    # - graph
    # - printsettings
    # - shell ('RemoteAdminPlugin' if i cant get it renamed)
    asyncio.run(amain())

"""
  debug          Used for development purpose
  # function was completely empty

  graph          Show channel graph
  # rarely used, but i like it so plugin it is

  printsettings  print the project's settings
  # fair and should be used more but meh, plugin too

  pyshell        Start ipython shell to send command to remote instance
  # garbage, use shell from RemoteAdminPlugin if at all

  pytest         start tests with pytest.
  # garbage, just use pytest bro wth

  shell          Start a custom shell to administrate remote pypeman...
  # name is annoyingly not "remoteadmin" but that's actualy fair

  start          Start pypeman as daemon (or foreground process)
  # actually used entry point, most flags are garbled tho

  startproject   Creates a pypeman project from scratch
  # plugin at best

  stop           stops an already running pypeman instance
  # meant to be used with "--daemon", ie unused

  test           Launch project's tests with unittest.main().
  # yeah alright buddy pal, take a break
"""
