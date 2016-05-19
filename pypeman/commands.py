#!/usr/bin/env python

# ############################################################################
# This is the code for THE pypeman command line program
# and all it's related commands
# after installing the module pypeman, all this functionality can be obtained 
# by calling 'pypeman' from the command line
# ############################################################################

import os
import sys


# Keep this import
sys.path.insert(0, os.getcwd())

import asyncio
import traceback
import importlib
import begin
import warnings
from functools import partial

from pypeman.helpers.reloader import reloader_opt
from pypeman import channels
from pypeman import nodes
from pypeman import endpoints

def load_project():
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
    except:
        import traceback

        traceback.print_exc()
        raise


def main(debug_asyncio=False, profile=False, cli=False):
    print('\nStart...')


    load_project()

    loop = asyncio.get_event_loop()

    if debug_asyncio:
        loop.set_debug(True)
        warnings.simplefilter('default')

    # Import modules for endpoints
    for end in endpoints.all:
        end.import_modules()

    # Import modules for nodes
    for node in nodes.all:
        node.import_modules()

    # Start channels
    for chan in channels.all:
        chan.import_modules()
        loop.run_until_complete(chan.start())

    # And endpoints
    for end in endpoints.all:
        loop.run_until_complete(end.start())

    # At the moment miing the asyncio and ipython
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


@begin.subcommand
def start(reload: 'Make server autoreload (Dev only)'=False, 
        debug_asyncio: 'Enable asyncio debug'=False,
        cli : "enables an IPython CLI for debugging (not perational)"=False,
        profile : "enables profiling / run stats (not operational)"=False,
        ):
    """ Start pypeman """
    reloader_opt(partial(main, debug_asyncio=debug_asyncio, cli=cli, profile=profile), reload, 2)


@begin.subcommand
def graph(dot: "Make dot compatible output"=False):
    """ Show channel graph """

    load_project()

    if dot:
        print("digraph testgraph{")
        for channel in channels.all:
            if not issubclass(channel.__class__, channels.SubChannel) and not issubclass(channel.__class__, channels.ConditionSubChannel):
                print("{node[shape=box]; %s; }" % channel.name)
                print(channel.name, end='')
                channel.graph_dot(previous=channel.name, end=channel.name)
        print("}")
    else:
        for channel in channels.all:
            if not issubclass(channel.__class__, channels.SubChannel) and not issubclass(channel.__class__, channels.ConditionSubChannel):
                print(channel.__class__.__name__)
                channel.graph()
                print('|-> out')
                print()


@begin.subcommand
def requirements():
    """ List optional python dependencies """

    load_project()

    dep = set()

    for channel in channels.all:
        dep |= set(channel.requirements())

    for node in nodes.all:
        dep |= set(node.requirements())

    for end in endpoints.all:
        dep |= set(end.requirements())

    [print(d) for d in dep]

@begin.subcommand
def startproject(dirname : "name of dir to install project to"):
    """ Creates a pypeman project from scrach """
    from pypeman.pjt_templates import new_project
    new_project(dirname)


@begin.subcommand
def debug():
    """ Used for development purpose """
    pass


@begin.start
def run(test=False, 

        ):
    """ Pypeman is a minimalistic but pragmatic ESB/ETL in python """
