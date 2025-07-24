"""Global graph related pypeman functions.
"""

import importlib
from logging import getLogger

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
    """removed and removing"""
    raise PypemanError("couldn't obtain graph's loop")
