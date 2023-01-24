#!/usr/bin/env python

""" Helpers for testing
- helpers for testing pypeman itself
- helpers for testing pypeman projects
"""

from unittest import TestCase

import asyncio
import logging

from pypeman import nodes
from pypeman import channels


logger = logging.getLogger(__name__)
# TODO implement settings override
# TODO implement MessageStoreMock


class PypeTestCase(TestCase):
    """ Test Case to be used for testing pypeman projects

        This test case ensures, that
        event loops and other asyncio and pypeman specifics
        are correctly cleaned up / set up between unit test
        runs.

        This class is necessary as asyncio and pypeman have
        some global persistent objects like
        asyncio default loop
        channels wait_subchans param (permits to raise exc in current loop)
        pypeman.nodes.all_nodes / pypeman.channels.all_channels, ...

        Anybody using unittest.TestCase based tests for a pypeman
        project should use this class instead.
    """

    loop = None
    wait_subchans = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addCleanup(self.cleanLoop)

    def setUp(self):
        if self.loop:
            raise Exception("a loop currently exists at setuP, not normal")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Start channels
        for chan in channels.all_channels:
            chan.loop = self.loop
            chan.wait_subchans = self.wait_subchans
            self.loop.run_until_complete(chan.start())
            chan._reset_test()

    def cleanLoop(self):
        for chan in channels.all_channels:
            self.loop.run_until_complete(chan.stop())
        self.loop.close()
        self.loop.stop()
        self.loop = None
        asyncio.set_event_loop(None)

    def finish_all_tasks(self):
        """
        You can use this function if you have some subchannel in you channel
        , don't have wait_subchans param set to True and anyway want to see the
        final result by processing all remaining tasks.

        :return: A list of raised exceptions during task execution.
        """
        if self.wait_subchans:
            logger.warning(
                "PypeTestCase.finish_all_tasks called when wait_subchans is set..."
                "useless, there won't be any pending tasks"
            )
        raised_exceptions = []
        pending = asyncio.all_tasks(loop=self.loop)

        for task in pending:
            try:
                self.loop.run_until_complete(task)
            except Exception as exc: # noqa
                raised_exceptions.append(exc)
        return raised_exceptions

    def get_channel(self, name):
        """
        Return a channel by is name. Remember to prepend with parent channel
        name for subchannel.

        :return: Channel instance corresponding to `name`
            or None if channel not found.
        """
        for chan in channels.all_channels:
            if chan.name == name:
                chan._reset_test()
                return chan
        raise NameError("Channel '%s' doesn't exist" % name)

    def set_loop_to_debug(self):
        """
        :return:
        """
        self.loop.set_debug(True)


class TearDownProjectTestCase(TestCase):
    """ unittest for tests creating a project

    this testcase ensures, that at the end of the test the project is
    removed
    """

    def tearDown(self):
        clear_pypeman_project()


def clear_pypeman_project():
    """ clear a pypeman project. should be used whenever tests don't need

        a certain pypeman graph anymore and one wants to be sure that
        residuals are cleaned away
    """

    nodes.reset_pypeman_nodes()
