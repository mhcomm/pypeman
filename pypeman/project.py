"""Loading of the user project.

The user project is a regular python module (`settings.PROJECT_MODULE`,
by default "project") which declares the channels, nodes and endpoints
when imported. See :func:`load_project`.
"""

import importlib
from logging import getLogger

from pypeman.conf import settings

logger = getLogger(__name__)


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
