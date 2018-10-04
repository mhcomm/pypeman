"""  Functionality, that can be used bys eveal test cases

"""

import asyncio
import datetime
import logging
import os
import time

from importlib import reload

from pypeman import nodes
from pypeman import message


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
        m.meta = default_message_meta
    else:
        m.meta = message_meta

    if with_context:
        # Add context message
        mctx = generate_msg(message_content={'question': 'known'}, message_meta={'answer': 43})
        m.add_context('test', mctx)

    return m


class TestException(Exception):
    """ custom Exception """


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
            await asyncio.sleep(self.async_delay, loop=self.loop)
        self.logger.info("Process done: %s", msg)
        self.processed = True
        return msg
