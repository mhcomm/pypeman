""" 
UnitTests checking, that debugging works as expected
"""

import os
import unittest
import asyncio

import logging

logger = logging.getLogger(__name__)

from pypeman.channels import BaseChannel
from pypeman import nodes

from pypeman.tests.common import TestException
from pypeman.tests.common import SimpleTestNode
from pypeman.tests.common import generate_msg
from pypeman.tests.common import setup_settings
from pypeman.tests.common import teardown_settings


SETTINGS_MODULE = 'pypeman.tests.tst_settings'


class MainLoopTests(unittest.TestCase):
    def setUp(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not shure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

    def tearDown(self):
        teardown_settings()

    def test_log(self):
        """ can modify log config """
        setup_settings(SETTINGS_MODULE)
        logger.debug("DEBUG")
        logger.info("INFO")
        logger.warning("WARNING")
        logger.error("ERROR")

    def test_no_log(self):
        pass

    #def test_loop_slow(self):
    #    """ main loop logs slow tasks """
    #    logger.debug("DEBUG")
    #    logger.info("INFO")
    #    logger.warning("WARNING")
    #    logger.error("ERROR")
    #    tst_logger = logging.getLogger('tests.debug.main_loop.slow')

    #    chan = BaseChannel(loop=self.loop)
    #    n1 = SimpleTestNode(delay=0.1, logger=tst_logger)
    #    n2 = SimpleTestNode(delay=0.9, logger=tst_logger)
    #    chan.add(n1)
    #    chan.add(n2)

    #    #msg = generate_msg()
    #    ## Launch channel processing
    #    #self.loop.run_until_complete(chan.start())
    #    #self.loop.run_until_complete(chan.handle(msg))


test_suite = MainLoopTests
