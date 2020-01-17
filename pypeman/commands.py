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

import click

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


CLI_CTX_SETTINGS = dict(help_option_names=['-h', '--help'])


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

    ctx = dict(
        logger=logger,
        loop=loop,
        )

    signal_handler = partial(sig_handler_func, ctx=ctx)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

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


@click.group()
@click.version_option(pypeman.__version__)
@click.help_option("--help", "-h")
def cli(version=False):
    """ Pypeman is a minimalistic but pragmatic ESB/ETL in python """


# some weird issue with new flake8 linters obliges us to add spaces before and after
# the '=' characters as soon as we add annotation strings
@cli.command(context_settings=CLI_CTX_SETTINGS)
@click.option(
    "--reload",
    is_flag=True,
    help="Make server autoreload (Dev only)",
    )
@click.option(
    "--debug-asyncio",
    is_flag=True,
    help="Enable asyncio debug",
    )
@click.option(
    "--cli",
    is_flag=True,
    help="enables an IPython CLI for debugging (not operational)",
    )
@click.option(
    "--remote-admin",
    is_flag=True,
    help="Enable remote admin server",
    )
@click.option(
    "--profile",
    is_flag=True,
    help="enables profiling / run stats (not operational)",
    )
@click.option(
    "--daemon/--no-daemon",
    default=True,
    help="started pypeman as daemon (default=True)",
    )
def start(reload, debug_asyncio, cli, remote_admin, profile, daemon):
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


@cli.command(context_settings=CLI_CTX_SETTINGS)
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


@cli.command(context_settings=CLI_CTX_SETTINGS)
@click.option(
    "--dot", is_flag=True,
    help="Make dot compatible output (Can be viewed with http://ushiroad.com/jsviz/)",
    )
def graph(dot):
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


@cli.command(context_settings=CLI_CTX_SETTINGS)
def pyshell():
    """ Start ipython shell to send command to remote instance """
    client = remoteadmin.RemoteAdminClient(
        url='ws://%s:%s' % (
            settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
            settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port']))
    client.init()

    from IPython import embed
    embed()


@cli.command(context_settings=CLI_CTX_SETTINGS)
def shell():
    """ Start a custom shell to administrate remote pypeman instance """
    settings.init_settings()
    try:
        remoteadmin.PypemanShell(url='ws://%s:%s' % (settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['host'],
                                 settings.REMOTE_ADMIN_WEBSOCKET_CONFIG['port'])).cmdloop()
    except KeyboardInterrupt:
        print('\nQuitting...')


@cli.command(context_settings=CLI_CTX_SETTINGS)
@click.argument("dirname")
def startproject(dirname):
    """ Creates a pypeman project from scratch

    DIRNAME: name of dir to install project to
    """
    from pypeman.pjt_templates import new_project
    new_project(dirname)


@cli.command(context_settings=CLI_CTX_SETTINGS)
def debug():
    """ Used for development purpose """
    pass


@cli.command(context_settings={
    "ignore_unknown_options": True, **CLI_CTX_SETTINGS})
@click.option(
    "--module", "-m",
    default="tests",
    help="the module parameter for unittest.main() (default: tests)",
    )
@click.argument("args", nargs=-1)
@click.pass_context
def test(ctx, module, args):
    """ Launch project's tests with unittest.main().
        All tests from one module (default 'tests') will be executed.

        MODULE: name of module (and its submodules) to test default=(tests)

        ARGS:   further args for unittest.main().
                To get more help type: pypeman test -- -h
    """
    args = [arg for arg in args if arg != "--"]
    from unittest import main

    load_project()

    # unittest.main should call sys.exit(). So no need to return status code
    # to caller level.
    # we could pass exit=False if we wanted to not call exit and do a custom
    # treatment
    main(module=module, argv=['pypeman test --'] + list(args))


@cli.command(context_settings=CLI_CTX_SETTINGS)
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


if __name__ == "__main__":
    cli()
