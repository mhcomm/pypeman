from unittest import TestCase
import asyncio
from pypeman import channels

# TODO implement settings override
# TODO implement MessageStoreMock


class PypeTestCase(TestCase):
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
        for chan in channels.all:
            chan.loop = cls.loop
            cls.loop.run_until_complete(chan.start())
            chan._reset_test()

    @classmethod
    def cleanLoop(cls):
        """
        Replace current loop by a new one to avoid side effect on
        next test.
        """
        for chan in channels.all:
            cls.loop.run_until_complete(chan.stop())

        pending = asyncio.Task.all_tasks(loop=cls.loop)
        asyncio.gather(*pending, loop=cls.loop).cancel()

        cls.loop.close()
        cls.loop = asyncio.new_event_loop()

        # Start channels
        for chan in channels.all:
            chan.loop = cls.loop
            cls.loop.run_until_complete(chan.start())
            chan._reset_test()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        # Stop channels
        for chan in channels.all:
            cls.loop.run_until_complete(chan.stop())

        cls.finish_all_tasks()

    @classmethod
    def finish_all_tasks(cls):
        """
        You can use this function if you have some subchannel in you channel
        and want to see the final result by processing all remaining tasks.

        :return: A list of raised exceptions during task execution.
        """
        raised_exceptions = []

        pending = asyncio.Task.all_tasks(loop=cls.loop)

        for task in pending:
            if not task.done():  # Exclude already resolved exception
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
        for chan in channels.all:
            if chan.name == name:
                chan._reset_test()
                return chan
        raise NameError("Channel '%s' doesn't exist" % name)

    def set_loop_to_debug(self):
        """
        :return:
        """
        self.loop.set_debug(True)
