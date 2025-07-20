"""
Global graph related pypeman functions.

On the long run some commands of pypeman.commands might be candidates
to be moved into this module
"""

import importlib
import time
from logging import getLogger

from .channels import all_channels
from .conf import settings
from .errors import PypemanError


logger = getLogger(__name__)


# XXX: if this is all that's left of pypeman.graph,
#      i'll be removing it asap
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
            channel = all_channels[0]
            # print("channel =", channel)
            loop = channel.loop
            break
        except Exception:
            if i == 0:
                raise PypemanError("couldn't obtain graph's loop")
        time.sleep(0.1)
    return loop
