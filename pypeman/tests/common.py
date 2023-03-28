"""  Functionality, that can be used bys eveal test cases

"""

import asyncio
import datetime
import logging
import os
import threading
import time

from importlib import reload

from pypeman import channels
from pypeman import endpoints
from pypeman import nodes
from pypeman import message

logger = logging.getLogger(__name__)


def setup_settings(module):
    """ helper allows to have specific settings for a test
    """
    os.environ['PYPEMAN_SETTINGS_MODULE'] = module
    import pypeman.default_settings
    import pypeman.conf
    reload(pypeman.default_settings)
    reload(pypeman.conf)
    import pypeman.tests.test_app.settings as tst_settings
    reload(tst_settings)
    from pypeman.conf import settings  # noqa: F401


def teardown_settings():
    """ helper allowing to reset settings to default
    """
    os.environ['PYPEMAN_SETTINGS_MODULE'] = 'pypeman.tests.settings.test_settings_default'

    # TODO: try later with del sys.modules[module_name]
    import logging
    logger = logging.getLogger()  # noqa: F841
    import pypeman.default_settings
    reload(pypeman.default_settings)
    try:
        import pypeman.conf
        reload(pypeman.conf)  # ########
    except ImportError:
        pass


default_message_content = """{"test":1}"""
default_message_meta = {'question': 'unknown'}


def generate_msg(timestamp=None, message_content=default_message_content,
                 message_meta=None, with_context=False):
    """ generates a default message """
    m = message.Message()
    if timestamp:
        if isinstance(timestamp, tuple):
            m.timestamp = datetime.datetime(*timestamp)
        else:  # assume it's a datetime object
            m.timestamp = timestamp
    else:  # just use current time
        m.timestamp = datetime.datetime.utcnow()

    m.payload = message_content

    if message_meta is None:
        m.meta = dict(default_message_meta)
    else:
        m.meta = message_meta

    if with_context:
        # Add context message
        mctx = generate_msg(message_content={'question': 'known'}, message_meta={'answer': 43})
        m.add_context('test', mctx)

    return m


class TstException(Exception):
    """ custom Exception """


class LongNode(nodes.ThreadNode):
    def process(self, msg):
        time.sleep(1)
        return msg


class StoreNode(nodes.BaseNode):
    """
    just store each payload in a buffer
    uses a threading lock to allow tests from another thread
    """
    def __init__(self, *args, **kwargs):
        super().__init__(args, **kwargs)
        self.lock = kwargs.pop("lock", None) or threading.Lock()
        self.payloads = []

    def process(self, msg):
        with self.lock:
            self.payloads.append(msg.payload)
        return msg


class SimpleTestNode(nodes.BaseNode):
    """ simple node, that can be used for unit testing
    """
    def __init__(self, *args, logger=None, loop=None, **kwargs):
        """
            :param delay: delay in milliseconds to simulate processing time
            :param logger: allows to pass a custom logger for tracing
        """
        self.delay = kwargs.pop('delay', 0)
        self.async_delay = kwargs.pop('async_delay', None)
        self.logger = logger or logging.getLogger(__name__)
        self.loop = loop
        super().__init__(*args, **kwargs)

        # Used to test if node is processed during test
        self.processed = False

    async def process(self, msg):
        if self.delay:
            time.sleep(self.delay)
        if self.async_delay is not None:
            await asyncio.sleep(self.async_delay)
        self.logger.info("Process done: %s", msg)
        self.processed = True
        return msg


class TstNode(nodes.BaseNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Used to test if node is processed during test

    def process(self, msg):
        print("Process %s" % self.name)
        return msg


class ExceptNode(TstNode):
    # This node raises an exception
    def process(self, msg):
        super().process(msg)
        raise TstException()


class MllPChannelTestThread(threading.Thread):
    """
    Start an MLLP Channel in a thread.
    Useful for test purpose
    can be stopped by calling .kill() and .join() methods

    MllpChannel Thread will auto destruct it to avoid infinite loops
    You can disable this feature with setting timeout arg to 0 or None

    params:
        chan_name: (str) name of the mllpChannel
        host: (str) default to '0.0.0.0'
        port: (int) default to 21000
        timeout: (float) default to 5 (5s)

    """
    def __init__(self, chan_name, host="0.0.0.0", port=21000, timeout=5):
        self.timeout = timeout
        self.async_loop = asyncio.new_event_loop()
        sock = f"{host}:{port}"
        endpoint = endpoints.MLLPEndpoint(loop=self.async_loop, sock=sock)
        self.chan = channels.MLLPChannel(
            name=chan_name, endpoint=endpoint, loop=self.async_loop)
        self.auto_kill_delayedtask = None if not timeout else threading.Timer(timeout, self._auto_kill)
        super().__init__(target=self.async_loop.run_forever)

    def start(self, *args, **kwargs):
        self.auto_kill_delayedtask.start()
        self.async_loop.run_until_complete(self.chan.start())
        self.async_loop.run_until_complete(self.chan.mllp_endpoint.start())
        super().start(*args, **kwargs)

    def _auto_kill(self):
        """
        Used to auto kill mllpChannel if it tooks too long time (call self.kill)
        """
        logger.warning(
            "mllp chan %s keeped alive too long time (timeout=%f), killing it",
            self.chan.name, self.timeout)
        self.kill()

    def kill(self):
        logger.debug("killing MllpChanTestThread %s", self.chan.name)
        if self.auto_kill_delayedtask and self.auto_kill_delayedtask.is_alive():
            self.auto_kill_delayedtask.cancel()
        pending_tasks = asyncio.all_tasks(loop=self.async_loop)
        for pending in pending_tasks:
            pending.cancel()
        self.async_loop.call_soon_threadsafe(self.async_loop.stop)
