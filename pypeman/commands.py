#!/usr/bin/env python

# ############################################################################
# This is the code for THE pypeman command line program
# and all it's related commands
# after installing the module pypeman, all this functionality can be obtained
# by calling 'pypeman' from the command line
# ############################################################################


import asyncio
import logging
import os
import signal
import sys
import warnings

from functools import partial

import click

from DaemonLite import DaemonLite

# Keep this change of the python path
if True:
    CURRENT_DIR = os.getcwd()  # noqa: E402
    sys.path.insert(0, CURRENT_DIR)  # noqa: E402

import pypeman

from pypeman import channels
from pypeman import endpoints
from pypeman import nodes
from pypeman import remoteadmin
from pypeman.conf import settings
from pypeman.graph import load_project
from pypeman.graph import mk_graph
from pypeman.helpers.reloader import reloader_opt


CLI_CTX_SETTINGS = dict(help_option_names=['-h', '--help'])


async def sig_handler_coro(loop, signal, ctx):
    """
    asyncio code handling the reception of signals
    """
    # TODO: is this really the right way to stop pypeman.
    # TODO: should'nt we just stop the loop and let the main() function do the rest.
    logger = ctx["logger"]
    logger.debug("signal handler coro called for signal %s", signal)
    # TODO: Check probably this strange magic was intended to allow stopping
    #   file watchers or any other tasks with a sleep.
    #   Let's debug with a file watcher (currently using a sleep)
    #   and a channel with a sleep
    #
    # probably interrupting sleepers and implementing a grace period should not be done
    # here, but in "main()"
    #
    # logger.debug("wait for a grace periond")
    # grace_period = 3
    # await asyncio.sleep(grace_period)
    logger.debug("now stop the loop")
    loop.stop()

    # I really hope we can remove this code. and handle everything in main
    # for channel in channels.all_channels:
    #     logger.debug("stop channel %s", channel.name)
    #     await channel.stop()
    # # sleep for a grace period of 3 seconds. Might be able to remove lateron
    # logger.debug("waiting for %ds to let channels stop", grace_period)
    # logger.debug("loop stopped")


def sig_handler_func(loop, signal, ctx):
    loop.create_task(sig_handler_coro(loop, signal, ctx))


# TODO: might rename main and refactor into pypman.graph
# could call it perhaps. run_graph() or load_and_run_graph()
def main(debug_asyncio=False, profile=False, cli=False, remote_admin=False):

    if debug_asyncio:
        # set before asyncio.get_event_loop() call
        os.environ['PYTHONASYNCIODEBUG'] = "1"

    load_project()
    print('\nStarting...')

    logger = logging.getLogger(__name__)
    loop = asyncio.get_event_loop()

    from pypeman.plugin_mgr import manager as plugin_manager
    plugin_manager.set_loop(loop)

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

    # Start plugins
    plugin_manager.start_plugins()

    # Start channels
    for chan in channels.all_channels:
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
    for end in endpoints.all_endpoints:
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
            plugin_manager=plugin_manager,
            settings=settings,
            print_graph=print_graph,
            )
        ctx["cli"] = cli
        ctx.update(namespace)
        cli = CLI(namespace=namespace)
        cli.run_as_thread()

    # https://github.com/mhcomm/pypeman/issues/149 (convert rmt admin to plugin)
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

    logger.debug("loop was stopped.")
    logger.debug("stop pypeman graph as nicely as possible")
    # https://github.com/mhcomm/pypeman/issues/150 ( ensure clean shutdown)

    for end in endpoints.all_endpoints:
        logger.debug("stop endpoint %s", repr(end))
        loop.run_until_complete(end.stop())

    # Stop all channels
    for chan in channels.all_channels:
        logger.debug("stop channel %s", repr(chan))
        loop.run_until_complete(chan.stop())

    # Stop all plugins
    plugin_manager.stop_plugins()

    print("End started tasks...")
    pending = asyncio.all_tasks()
    logger.debug("%d pending tasks to wait for", len(pending))
    for task in pending:
        logger.debug("shall wait for task %s", repr(task))
    # TODO: This is where we might have to add a grace period.
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


@cli.command(context_settings=CLI_CTX_SETTINGS)
def printsettings():
    """
    print the project's settings

    """
    # TODO: This command could be enhanced further to be more simniliar to
    #       django_extensions' print_settings
    load_project()
    svars = sorted([val for val in dir(settings) if "A" <= val[0] <= "Z"])
    for name in svars:
        print(name, "=", repr(getattr(settings, name)))


def print_graph(dot=False):
    """ prints the project graph """
    for line in mk_graph(dot):
        print(line)


@cli.command(context_settings=CLI_CTX_SETTINGS)
@click.option(
    "--dot", is_flag=True,
    help="Make dot compatible output (Can be viewed with http://ushiroad.com/jsviz/)",
    )
def graph(dot):
    """ Show channel graph"""

    load_project()
    print_graph(dot)


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


@cli.command(context_settings={
    "ignore_unknown_options": True, **CLI_CTX_SETTINGS})
@click.argument("args", nargs=-1)
@click.pass_context
def pytest(ctx, args):
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
    sys.exit(rslt)


if __name__ == "__main__":
    cli()
