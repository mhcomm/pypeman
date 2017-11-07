#!/usr/bin/env python

# ############################################################################
# This is the code for THE pypeman command line program
# and all it's related commands
# after installing the module pypeman, all this functionality can be obtained
# by calling 'pypeman' from the command line
# ############################################################################

import os
import sys

# TODO: remove below if statement asap. This is a workaround for a bug in begins
# TODO: which provokes an eception when calling pypeman without parameters.
# TODO: more info at https://github.com/aliles/begins/issues/48
if len(sys.argv) == 1:
    sys.argv.append('-h')


# Keep this import
sys.path.insert(0, os.getcwd())

import asyncio
import traceback
import importlib
import begin
import warnings
from functools import partial

from DaemonLite import DaemonLite

import pypeman
from pypeman.helpers.reloader import reloader_opt
from pypeman import channels
from pypeman import nodes
from pypeman import endpoints
from pypeman.conf import settings

def load_project():
    settings.init_settings()
    try:
        importlib.import_module('project')
    except ImportError as exc:
        msg = str(exc)
        if not 'No module' in msg:
            print("IMPORT ERROR project")
            raise
        if not 'project' in msg:
            print("IMPORT ERROR project")
            raise
        print("Missing 'project.py' file !")
        sys.exit(-1)
    except:
        traceback.print_exc()
        raise


def main(debug_asyncio=False, profile=False, cli=False):

    if debug_asyncio:
        # set before asyncio.get_event_loop() call
        os.environ['PYTHONASYNCIODEBUG'] = "1"

    load_project()
    print('\nStarting...')

    loop = asyncio.get_event_loop()

    if debug_asyncio:
        loop.slow_callback_duration = settings.DEBUG_PARAMS['slow_callback_duration']
        loop.set_debug(True)
        warnings.simplefilter('default')

    # Start channels
    for chan in channels.all:
        loop.run_until_complete(chan.start())

    # And endpoints
    for end in endpoints.all:
        loop.run_until_complete(end.start())

    # At the moment mixing the asyncio and ipython
    # event loop is not working properly.
    # Thus ipython
    if cli:
        from pypeman.helpers.cli import CLI
        namespace = dict(
            loop=loop,
            nodes=nodes,
            endpoints=endpoints,
            channels=channels,
            )
        cli = CLI(namespace=namespace)
        cli.run_as_thread()

    print('Waiting for messages...')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    print("End started tasks...")

    for chan in channels.all:
        loop.run_until_complete(chan.stop())

    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))

    loop.close()


def mk_daemon(mainfunc=lambda: None, pidfile="pypeman.pid"):
    # TODO: might move to a separate module like e.g.  pypeman.helpers.daemon
    # might also look at following alternative modules:
    # - python-daemon
    # - daemonocle
    # - py daemoniker
    # Alternatively if we don't want other module dependencies we might just copy
    # the DaemonLite files into your source repository
    class DaemonizedApp(DaemonLite):
        def run(self):
            mainfunc()
    app = DaemonizedApp(pidfile)
    return app


@begin.subcommand
def start(reload: 'Make server autoreload (Dev only)'=False,
          debug_asyncio: 'Enable asyncio debug'=False,
          cli : "enables an IPython CLI for debugging (not operational)"=False,
          profile : "enables profiling / run stats (not operational)"=False,
          # TODO: can be True if DaemonLite is a hard requirement
          daemon : "if true pypeman will be started as daemon "=True,
        ):
    """ Start pypeman as daemon (or foreground process) """
    main_func = partial(main, debug_asyncio=debug_asyncio, cli=cli,
            profile=profile)
    start_func = partial(reloader_opt, main_func, reload, 2)

    if reload:
        start_func()
    else:
        if daemon:
            daemon = mk_daemon(main_func)
            daemon.start()
        else:
            main_func()


@begin.subcommand
def stop():
    """ stops an already running pypeman instance """
    daemon = mk_daemon()
    daemon.stop()

@begin.subcommand
def graph(dot: "Make dot compatible output (Can be viewed with http://ushiroad.com/jsviz/)"=False):
    """ Show channel graph"""

    load_project()

    if dot:
        print("digraph testgraph{")

        # Handle channel node shape
        for channel in channels.all:
            print('{node[shape=box]; "%s"; }' % channel.name)

        # Draw each graph
        for channel in channels.all:
            if not channel.parent:
                channel.graph_dot()

        print("}")
    else:
        for channel in channels.all:
            if not channel.parent:
                print(channel.__class__.__name__)
                channel.graph()
                print('|-> out')
                print()


@begin.subcommand
def startproject(dirname : "name of dir to install project to"):
    """ Creates a pypeman project from scrach """
    from pypeman.pjt_templates import new_project
    new_project(dirname)


@begin.subcommand
def debug():
    """ Used for development purpose """
    pass


@begin.subcommand
def test():
    """ Launch project tests """
    from unittest import main

    load_project()

    main(module='tests', argv=['pypeman'])


@begin.start
def run(version=False):
    """ Pypeman is a minimalistic but pragmatic ESB/ETL in python """
    if version:
        print(pypeman.__version__)
        sys.exit(0)
