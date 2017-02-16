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

class EvtLoopMixin:
    def setup_evt_loop(self):
        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not shure)
        self.loop = asyncio.new_event_loop()
        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

class LoggingTests(unittest.TestCase, EvtLoopMixin):
    def setUp(self):
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
        setup_settings(SETTINGS_MODULE) 
        teardown_settings()

    def test_log(self):
        """ can modify log config 

            changing log config may be important for other tests.
            thus let's try to change it and see its impact.
        """

        self.setup_evt_loop()
        logger = self.logger # get default test logger
        handler = self.loghandler # get test log handler


        cnt0 = handler.num_entries()
        # test second logger
        logger.debug("DEBUG")
        self.assertEqual(handler.num_entries()-cnt0, 0, "A0: expected 0 log entry")
        logger.info("INFO")
        self.assertEqual(handler.num_entries()-cnt0, 0, "A1: expected 0 log entry")
        logger.warning("WARNING")
        self.assertEqual(handler.num_entries()-cnt0, 1, "A2: expected 1 log entry")
        logger.error("ERROR")

        # now reconfigure logging
        from pypeman.helpers.logging import DebugLogHandler
        setup_settings(SETTINGS_MODULE2) # adapt config
        from pypeman.conf import settings
        _logging = settings.LOGGING # force loading of cfg
        # create logger and add custom handler
        logger = logging.getLogger()
        handler = DebugLogHandler()
        logger.handlers.append(handler)

        cnt0 = handler.num_entries()
        # test first logger
        logger.debug("DEBUG")
        self.assertEqual(handler.num_entries()-cnt0, 1, "B: expected 1 log entry")
        logger.info("INFO")
        self.assertEqual(handler.num_entries()-cnt0, 2, "B: expected 2 log entries")

    def test_no_log(self):
        pass

class MainLoopTests(unittest.TestCase, EvtLoopMixin):
    def setUp(self):
        """ """
        self.prev_debug_flag = os.environ.get('PYTHONASYNCIODEBUG', None)
        setup_settings(SETTINGS_MODULE) # adapt config
        from pypeman.conf import settings
        _logging = settings.LOGGING # force loading of cfg

        # create logger and add custom handler
        #self.logger = logging.getLogger()
        #print("got logger")
        #from pypeman.helpers.logging import DebugLogHandler
        #self.loghandler = handler = DebugLogHandler()
        #self.logger.handlers.append(handler)
        #print("added handler")

    def tearDown(self):
        if self.prev_debug_flag is not None:
            os.environ['PYTHONASYNCIODEBUG'] = self.prev_debug_flag
        else:
            del os.environ['PYTHONASYNCIODEBUG']
        setup_settings(SETTINGS_MODULE) 
        teardown_settings()


    def test_loop_slow(self):
        """ main loop logs slow tasks """
        import pypeman.debug
        pypeman.debug.enable_slow_log_stats()
        self.setup_evt_loop()

        #logger = self.logger # get default test logger
        #handler = self.loghandler # get test log handler

        tst_logger = logging.getLogger('tests.debug.main_loop.slow')
        print(tst_logger.handlers)

        chan = BaseChannel(loop=self.loop)
        n1 = SimpleTestNode(delay=0.101, logger=tst_logger)
        n2 = SimpleTestNode(delay=0.109, logger=tst_logger)
        chan.add(n1)
        chan.add(n2)

        msg = generate_msg()
        # Launch channel processing
        self.loop.run_until_complete(chan.start())
        self.loop.run_until_complete(chan.handle(msg))
        #handler.show_entries()
        pypeman.debug.show_slow_log_stats()


#test_suite = MainLoopTests
