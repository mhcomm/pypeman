# pypeman -- A command-line argument parser for Python
# Copyright (C) 2015-2016 by MHComm. See LICENSE for details

import logging
from logging.config import dictConfig

from pypeman.test import TearDownProjectTestCase as TestCase


class DefaultSettingsTests(TestCase):
    def test_01_default_settings(self):
        """ default settings exists """
        import pypeman.default_settings as dflt_settings  # noqa: F401

    def test_02_default_logging(self):
        """ default_logging is valid """
        import pypeman.default_settings as dflt_settings
        dictConfig(dflt_settings.LOGGING)
        logger = logging.getLogger(__name__)
        logger.debug("debug")
        logger.info("info")
        logger.warning("warning")
        logger.error("error")
        logger.critical("critical")
