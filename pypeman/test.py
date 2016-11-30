from unittest import TestCase
import asyncio
from pypeman import channels

# TODO implement settings overload
# TODO implement MessageStoreMock

class PypeTestCase(TestCase):
    loop = None

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
    def tearDownClass(cls):
        super().tearDownClass()

        # Stop channels
        for chan in channels.all:
            cls.loop.run_until_complete(chan.stop())

        cls.finish_all_tasks()

    @classmethod
    def finish_all_tasks(cls):

        # Useful to execute future callbacks
        pending = asyncio.Task.all_tasks(loop=cls.loop)

        if pending:
            cls.loop.run_until_complete(asyncio.gather(*pending))

    def get_channel(self, name):
        for chan in channels.all:
            if chan.name == name:
                chan._reset_test()
                return chan
        return None

    def set_loop_to_debug(self):
        """
        :return:
        """
        self.loop.set_debug(True)


