"""
Base class for a pypeman plugin

All interfaces are specified nothing is implemented
"""

import logging

from inspect import iscoroutinefunction


logger = logging.getLogger(__name__)


class BasePlugin:
    (INITIALIZED, READY, STARTING, STARTED, STOPPING, STOPPED, DESTROYED) = \
        states = range(7)

    STATE_NAMES = (
        "INITIALIZED", "READY", "STARTING", "STARTED",
        "STOPPING", "STOPPED", "DESTROYED",
        )

    def __init__(self):
        """
        Early initialisation before any pypeman endpoints and channels are created
        """
        self.name = f"{__name__}.{self.__class__}"
        logger.debug("Initialing plugin %s", self.name)
        self.status = self.INITIALIZED
        self.loop = None
        self.to_start = []
        self.start_results = []

    def do_ready(self):
        """
        Code to be executed after the whole pypeman graph has been generated.
        ready() functions of all plugins are executed before the loop is generated
        """
        logger.debug("called ready for plugin %s", self.name)
        assert self.status == self.INITIALIZED
        self.ready()
        self.status = self.READY

    def ready(self):
        pass

    def set_loop(self, loop):
        self.loop = loop

    def set_starting(self):
        logger.debug("starting plugin %s", self.name)
        assert self.status == self.READY
        self.status = self.STARTING

    def set_started(self):
        self.status = self.STARTED

    async def do_start(self):
        """
        Code to be executed after the creation of the event loop
        """
        self.set_starting()
        if iscoroutinefunction(self.start):
            logger.debug("awaiting coro start()")
            future = await self.start()
            logger.debug("started => %s", repr(future))
            self.start_results.append(future)
        else:
            start_rslt = self.start()
            logger.debug("start rslt = %s", repr(start_rslt))
            if isinstance(start_rslt, (list, tuple)):
                logger.debug("start_rslt is no coro")
                for coro in start_rslt:
                    rslt = await coro
                    self.start_results.append(rslt)
            elif start_rslt is None:
                logger.debug("start_rslt is None")
            elif iscoroutinefunction(start_rslt):
                logger.debug("start_rslt is a coro")
                rslt = await start_rslt()
                logger.debug("rslt: %s", repr(rslt))
                self.start_results.append(rslt)
            else:
                logger.debug("start_rslt is awaitable")
                rslt = await start_rslt
                logger.debug("rslt: %s", repr(rslt))
                self.start_results.append(rslt)
        self.set_started()
        logger.debug("end of start")

    async def start(self):
        pass

    def start_coro_list(self):
        """
        alternative start implementation returning a list of coros to be executed.
        """
        return [self.do_start()]

    async def do_stop(self):
        """
        Code to be executed before mainloop shall be stopped
        """
        logger.debug("stopping plugin %s", self.name)
        assert self.status in (self.INITIALIZED, self.STARTING, self.STARTED)
        if self.status == self.STARTING:
            # TODO: should change lateron, that a starting plugin can also
            #       be stopped
            raise RuntimeError("had to wait till started before stopping")
        elif self.status == self.STARTED:
            if iscoroutinefunction(self.stop):
                logger.debug("awaiting coro stop()")
                await self.stop()
            else:
                coro = self.stop()
                assert iscoroutinefunction(coro)
                await coro
            self.status = self.STOPPED

    async def stop(self):
        pass

    def do_destroy(self):
        """
        Code to be executed after all plugins are stopped
        """
        logger.debug("destroying plugin %s", self.name)
        assert self.status == self.STOPPED
        self.destroy()

    def destroy(self):
        assert self.status in (self.INITIALIZED, self.STOPPED)
        self.status = self.DESTROYED
