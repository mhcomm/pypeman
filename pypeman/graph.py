"""
Global graph related pypeman functions.

On the long run some commands of pypeman.commands might be candidates
to be moved into this module
"""


import importlib
import sys
import time
import traceback

from pypeman.conf import settings
from pypeman import channels
from pypeman.errors import PypemanError


def load_project():
    settings.init_settings()
    from pypeman.plugin_mgr import manager as plugin_manager
    plugin_manager.import_plugins()
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
    plugin_manager.init_plugins()
    plugin_manager.ready_plugins()


def wait_for_loop(tmax=5.0):
    """
    wait until the loop variable of a pypeman graph
    has been initialized

    can be used from any thread that's not the main thread
    """
    # TODO: might factor out this function to a helper module
    loop = None
    steps = int(tmax / 0.1)
    for i in range(steps, -1, -1):
        try:
            channel = channels.all_channels[0]
            # print("channel =", channel)
            loop = channel.loop
            break
        except Exception:
            if i == 0:
                raise PypemanError("couldn't obtain graph's loop")
        time.sleep(0.1)
    return loop


def mk_ascii_graph(title=None):
    """ Show pypeman graph as ascii output.
        Better reuse for debugging or new code
    """
    if title:
        yield title
    for channel in channels.all_channels:
        if not channel.parent:
            yield channel.__class__.__name__
            for entry in channel.graph():
                yield entry
            yield "|-> out"
            yield ""


def mk_graph(dot=False):
    if dot:
        yield "digraph testgraph{"

        # Handle channel node shape
        for channel in channels.all_channels:
            yield '{node[shape=box]; "%s"; }' % channel.name

        # Draw each graph
        for channel in channels.all_channels:
            if not channel.parent:
                for line in channel.graph_dot():
                    yield line

        yield "}"
    else:
        for line in mk_ascii_graph():
            yield line
