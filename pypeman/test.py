#!/usr/bin/env python

""" Helpers for testing
- helpers for testing pypeman itself
- helpers for testing pypeman projects
"""

from unittest import TestCase

import asyncio

from pypeman import nodes
from pypeman import channels


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
        pypeman.nodes.all_nodes / pypeman.channels.all_channels, ...

        Anybody using unittest.TestCase based tests for a pypeman
        project should use this class instead.
    """

    loop = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addCleanup(self.cleanLoop)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Create class event loop used for tests to avoid failing
        # previous tests to impact next test ? (Not sure)
        cls.loop = asyncio.new_event_loop()

        # Remove thread event loop to be sure we are not using
        # another event loop somewhere
        asyncio.set_event_loop(None)

        # Start channels
        for chan in channels.all_channels:
            chan.loop = cls.loop
            cls.loop.run_until_complete(chan.start())
            chan._reset_test()

    @classmethod
    def cleanLoop(cls):
        """
        Replace current loop by a new one to avoid side effect on
        next test.
        """
        for chan in channels.all_channels:
            cls.loop.run_until_complete(chan.stop())

        pending = asyncio.all_tasks(loop=cls.loop)
        if pending:
            asyncio.gather(*pending).cancel()

        cls.loop.close()
        cls.loop = asyncio.new_event_loop()

        # Start channels
        for chan in channels.all_channels:
            chan.loop = cls.loop
            cls.loop.run_until_complete(chan.start())
            chan._reset_test()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        # Stop channels
        for chan in channels.all_channels:
            cls.loop.run_until_complete(chan.stop())

        cls.finish_all_tasks()

    @classmethod
    def finish_all_tasks(cls):
        """
        You can use this function if you have some subchannel in you channel
        and want to see the final result by processing all remaining tasks.
        TODO: maybe remove this function (all subchannels are awaited in the
            handle so pending will be empty)

        :return: A list of raised exceptions during task execution.
        """
        raised_exceptions = []

        pending = asyncio.all_tasks(loop=cls.loop)

        for task in pending:
            try:
                cls.loop.run_until_complete(task)
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
