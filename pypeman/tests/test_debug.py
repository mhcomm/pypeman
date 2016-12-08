""" 
UnitTests checking, that debugging works as expected
"""

import os
import unittest
import asyncio

import logging

from pypeman.channels import BaseChannel
from pypeman import nodes

from pypeman.tests.common import TestException
from pypeman.tests.common import SimpleTestNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import setup_settings
from pypeman.tests.common import teardown_settings



SETTINGS_MODULE = 'pypeman.tests.test_app.settings'
SETTINGS_MODULE2 = 'pypeman.tests.test_app.settings2'


class MainLoopTests(unittest.TestCase):
    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not shure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

        setup_settings(SETTINGS_MODULE) # adapt config
        from pypeman.conf import settings
        _logging = settings.LOGGING # force loading of cfg
        # create logger and add custom handler
        self.logger = logging.getLogger()
        print("got logger")
        from pypeman.helpers.logging import DebugLogHandler
        self.loghandler = handler = DebugLogHandler()
        self.logger.handlers.append(handler)
        print("added handler")

    def tearDown(self):
        teardown_settings()

    def test_log(self):
        """ can modify log config 

            changing log config may be important for other tests.
            thus let's try to change it and see its impact.
        """

        logger = self.logger # get default test logger
        handler = self.loghandler # get test log handler


        # test first logger
        logger.debug("DEBUG")
        self.assertEqual(handler.num_entries(), 1, "expected 1 log entry")
        logger.info("INFO")
        self.assertEqual(handler.num_entries(), 2, "expected 2 log entries")

        # now reconfigure logging
        from pypeman.helpers.logging import DebugLogHandler
        setup_settings(SETTINGS_MODULE2) # adapt config
        from pypeman.conf import settings
        _logging = settings.LOGGING # force loading of cfg
        # create logger and add custom handler
        logger = logging.getLogger()
        handler = DebugLogHandler()
        logger.handlers.append(handler)

        # test second logger
        logger.debug("DEBUG")
        self.assertEqual(handler.num_entries(), 0, "expected 0 log entry")
        logger.info("INFO")
        self.assertEqual(handler.num_entries(), 0, "expected 0 log entry")
        logger.warning("WARNING")
        self.assertEqual(handler.num_entries(), 1, "expected 1 log entry")
        logger.error("ERROR")

    def test_no_log(self):
        pass

    def test_loop_slow(self):
        """ main loop logs slow tasks """
    #    logger.debug("DEBUG")
    #    logger.info("INFO")
    #    logger.warning("WARNING")
    #    logger.error("ERROR")
        handler = self.loghandler # get test log handler
        tst_logger = logging.getLogger('tests.debug.main_loop.slow')
        print(tst_logger.handlers)

        chan = BaseChannel(loop=self.loop)
        n1 = SimpleTestNode(delay=0.1, logger=tst_logger)
        n2 = SimpleTestNode(delay=0.9, logger=tst_logger)
        chan.add(n1)
        chan.add(n2)

        msg = generate_msg()
        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))
        handler.show_entries()


#test_suite = MainLoopTests
