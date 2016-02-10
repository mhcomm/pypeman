#!/usr/bin/env python

# ############################################################################
# This is the code for THE pypeman command line program
# and all it's related commands
# after installing the module pypeman, all this functionality can be obtained 
# by calling 'pypeman' from the command line
# ############################################################################

import os
import sys

# workaround for allowing this script to be called as pypeman
# TODO: might set __name__ only if called from pypeman wrapper
__name__ =  '__main__'

# Keep this import
sys.path.insert(0, os.getcwd())

import asyncio
import traceback
import importlib
import begin

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


def main():
    print('Start...')

    loop = asyncio.get_event_loop()

    load_project()

    # Import modules for nodes
    for node in nodes.all:
        node.import_modules()

    # Start channels
    for chan in channels.all:
        chan.import_modules()
        loop.run_until_complete(chan.start())

    # And endpoints
    for end in endpoints.all:
        end.import_modules()
        loop.run_until_complete(end.start())

    print('Waiting for connection...')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()


@begin.subcommand
def start(reload: 'Make server autoreload (Dev only)'=False):
    """ Start pypeman """
    reloader_opt(main, reload, 2)
    sys.exit(0) # workaround till we know, why new wrapper always waits for connections


@begin.subcommand
def graph():
    """ Show channel graph """

    load_project()

    for channel in channels.all:
        if not issubclass(channel.__class__, channels.SubChannel) and not issubclass(channel.__class__, channels.ConditionSubChannel):
            print(channel.__class__.__name__)
            channel.graph()
            print('|-> out')
            print()
    sys.exit(0) # workaround till we know, why new wrapper always waits for connections


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
    sys.exit(0) # workaround till we know, why new wrapper always waits for connections

@begin.subcommand
def startproject(dirname : "name of dir to install project to"):
    """ creates a pypeman project from scrach """
    os.makedirs(dirname)
    sys.exit(0) # workaround till we know, why new wrapper always waits for connections


@begin.subcommand
def debug():
    """ Used for development purpose """
    pass


@begin.start
def run():
    """ Pypeman is a minimalistic but pragmatic ESB/ETL in python """
