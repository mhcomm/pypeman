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
