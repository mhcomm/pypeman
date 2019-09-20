#!/usr/bin/env python

# ############################################################################
# This is the code for THE pypeman command line program
# and all it's related commands
# after installing the module pypeman, all this functionality can be obtained
# by calling 'pypeman' from the command line
# ############################################################################


import asyncio
import importlib
import logging
import os
import traceback
import signal
import sys
import warnings

from functools import partial

import begin

from DaemonLite import DaemonLite

# Keep this change of the python path
CURRENT_DIR = os.getcwd()  # noqa: E402
sys.path.insert(0, CURRENT_DIR)  # noqa: E402

# To be imported prior to any other pypeman imports
import pypeman.helpers.aio_compat
pypeman.helpers.aio_compat.patch()  # noqa: E402

import pypeman

from pypeman import channels
from pypeman import endpoints
from pypeman import nodes
from pypeman import remoteadmin
from pypeman.conf import settings
from pypeman.helpers.reloader import reloader_opt


# TODO: remove below if statement asap. This is a workaround for a bug in begins
# TODO: which provokes an exception when calling pypeman without parameters.
# TODO: more info at https://github.com/aliles/begins/issues/48

if len(sys.argv) == 1:
    sys.argv.append('-h')


async def sig_handler_coro(loop, signal, ctx):
    """
    asyncio code handling the reception of signals
    """
    logger = ctx["logger"]
    logger.debug("signal handler coro called for signal %s", signal)
    for channel in channels.all:
        logger.debug("stop channel %s", channel.name)
        await channel.stop()
    # sleep for a grace period of 3 seconds. Might be able to remove lateron
    grace_period = 3
    logger.debug("waiting for %ds to let channels stop", grace_period)
    await asyncio.sleep(grace_period)
    loop.stop()
    logger.debug("loop stopped")


def sig_handler_func(loop, signal, ctx):
    loop.create_task(sig_handler_coro(loop, signal, ctx))


def load_project():
    settings.init_settings()
    project_module = settings.PROJECT_MODULE
    try:
        importlib.import_module(project_module)
    except ImportError as exc:
        msg = str(exc)
        if 'No module' not in msg:
            print("IMPORT ERROR %s" % project_module)
            raise
        if project_module not in msg:
            print("IMPORT ERROR %s" % project_module)
            raise
        print("Missing '%s' module !" % project_module)
        sys.exit(-1)
    except Exception:
        traceback.print_exc()
        raise


def main(debug_asyncio=False, profile=False, cli=False, remote_admin=False):

    if debug_asyncio:
        # set before asyncio.get_event_loop() call
        os.environ['PYTHONASYNCIODEBUG'] = "1"

    load_project()
    print('\nStarting...')

    logger = logging.getLogger(__name__)
    loop = asyncio.get_event_loop()

    if debug_asyncio:
        loop.slow_callback_duration = settings.DEBUG_PARAMS['slow_callback_duration']
        loop.set_debug(True)
        warnings.simplefilter('default')

    # Start channels
    for chan in channels.all:
        loop.run_until_complete(chan.start())

    # add signal handlers
    ctx = dict(  # a context dict to easily add more params if needed
        logger=logger,
        )

    for sig in (
            signal.SIGINT,  # CTRL-C
            signal.SIGTERM,  # Termination (kill w/o params)
            # SIGHUP,  # unused could use to reread / conf / reload nicely
            # SUGUSR1,  # could be used to print / dump debug info
            ):
        loop.add_signal_handler(sig, partial(sig_handler_func, loop, sig, ctx))

    # And endpoints
    for end in endpoints.all:
        loop.run_until_complete(end.start())

    # At the moment mixing the asyncio and ipython
    # event loop is not working properly.
    # Thus ipython is executed in a separate thread
    if cli:
        from pypeman.helpers.cli import CLI
        namespace = dict(
            fake_signal_int=partial(sig_handler_func, signal.SIGINT, None),
            asyncio=asyncio,
            logger=logger,
            loop=loop,
            nodes=nodes,
            endpoints=endpoints,
            channels=channels,
            )
        ctx["cli"] = cli
        ctx.update(namespace)
        cli = CLI(namespace=namespace)
        cli.run_as_thread()

    if remote_admin:
        remote = remoteadmin.RemoteAdminServer(loop=loop, **settings.REMOTE_ADMIN_WEBSOCKET_CONFIG)
        loop.run_until_complete(remote.start())

        print("Remote admin websocket started at {host}:{port}".format(
            **settings.REMOTE_ADMIN_WEBSOCKET_CONFIG
        ))

        webadmin = remoteadmin.WebAdmin(loop=loop, **settings.REMOTE_ADMIN_WEB_CONFIG)

        loop.run_until_complete(webadmin.start())

        print("Web remote admin started at {host}:{port}".format(
            **settings.REMOTE_ADMIN_WEB_CONFIG
        ))

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


# some weird issue with new flake8 linters obliges us to add spaces before and after
# the '=' characters as soon as we add annotation strings
@begin.subcommand  # noqa: F722
def start(reload: 'Make server autoreload (Dev only)' = False,
          debug_asyncio: 'Enable asyncio debug' = False,
          cli: "enables an IPython CLI for debugging (not operational)" = False,
          remote_admin: 'Enable remote admin server' = False,
          profile: "enables profiling / run stats (not operational)" = False,
          daemon: "if true pypeman will be started as daemon " = True):
    """ Start pypeman as daemon (or foreground process) """

    main_func = partial(
        main,
        debug_asyncio=debug_asyncio,
        cli=cli,
        profile=profile,
        remote_admin=remote_admin
    )
    start_func = partial(reloader_opt, main_func, reload, 2)

    daemon = (os.environ.get('PYPEMAN_NO_DAEMON') != "True") and daemon

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


def show_ascii_graph(title=None):
    """ Show pypeman graph as ascii output.
        Better reuse for debugging or new code
    """
    if title:
        print(title)
    for channel in channels.all:
        if not channel.parent:
            print(channel.__class__.__name__)
            channel.graph()
            print('|-> out')
            print()


# some weird issue with new flake8 linters obliges us to add spaces before and after
# the '=' characters as soon as we add annotation strings
@begin.subcommand  # noqa: F722
def graph(dot: "Make dot compatible output (Can be viewed with http://ushiroad.com/jsviz/)" = False):
    """ Show channel graph"""

    load_project()

    if dot:
        print("digraph testgraph{")

        # Handle channel node shape
        for channel in channels.all:
            print('{node[shape=box]; "%s"; }' % channel.name)

        # Draw each graph
        for channel in channels.all:
            if not channel.parent:
                channel.graph_dot()

        print("}")
    else:
        show_ascii_graph()


@begin.subcommand
def pyshell():
    """ Start ipython shell to send command to remote instance """
    client = remoteadmin.RemoteAdminClient(url='ws://%s:%s' % (settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
                                           settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port']))
    client.init()

    from IPython import embed
    embed()


@begin.subcommand
def shell():
    """ Start a custom shell to administrate remote pypeman instance """
    settings.init_settings()
    try:
        remoteadmin.PypemanShell(url='ws://%s:%s' % (settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
                                 settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port'])).cmdloop()
    except KeyboardInterrupt:
        print('\nQuitting...')


@begin.subcommand  # noqa: F722
def startproject(dirname: "name of dir to install project to"):
    """ Creates a pypeman project from scrach """
    from pypeman.pjt_templates import new_project
    new_project(dirname)


@begin.subcommand
def debug():
    """ Used for development purpose """
    pass


@begin.subcommand  # noqa: F722
def test(module: "the module parameter for unittest.main()" = "tests",
         *args: "further args for unittest.main(). "
                "To get more help type: pypeman test -- -h"):
    """ Launch project's tests with unittest.main().
        All tests from one module (default 'tests') will be executed.
    """
    from unittest import main

    load_project()

    # unittest.main should call sys.exit(). So no need to return status code
    # to caller level.
    # we could pass exit=False if we wanted to not call exit and do a custom
    # treatment
    main(module=module, argv=['pypeman test --'] + list(args))


@begin.subcommand
def pytest(*args):
    """ start tests with pytest.
        Params can be passed through with -- [args...].
        Pytest help with:
        pypeman pytest -- -h
    """
    import pytest

    load_project()
    if not args:
        args = ["tests.py"]
    rslt = pytest.main(list(args))
    return rslt  # required to return the error code of pytest


@begin.start
def run(version=False):
    """ Pypeman is a minimalistic but pragmatic ESB/ETL in python """
    if version:
        print(pypeman.__version__)
        sys.exit(0)
