# This is the centralized settings File For pypeman
# This file should contain all defaults settings, that should be added to your version control
# System

# At the moment we really don't have anything useful project wide.
# Let's think about a good example

import os

# next line required, such, that settings variables (like LOGGING)
# can be imported **and** enhanced
from pypeman.default_settings import *  # noqa: F401, F403

# settings var pointing to project's directory
PJT_DIR = os.path.realpath(os.path.dirname(__file__))
