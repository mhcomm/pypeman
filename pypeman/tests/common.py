"""  Functionality, that can be used bys eveal test cases

"""

import os
import time
import datetime
import logging
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
    from pypeman.conf import settings

def teardown_settings():
    """ helper allowing to reset settings to default 
    """
    os.environ['PYPEMAN_SETTINGS_MODULE'] = 'pypeman.tests.settings.test_settings_default'

    # TODO: try later with del sys.modules[module_name]
    import logging
    logger = logging.getLogger()
    import pypeman.default_settings
    reload(pypeman.default_settings)
    try:
        import pypeman.conf
        reload(pypeman.conf) #########
    except ImportError:
        pass

default_message_content = """{"test":1}"""
def generate_msg(timestamp=None, message_content=default_message_content):
    """ generates a default message """
    m = message.Message()
    if timestamp:
        if isinstance(timestamp, tuple):
            m.timestamp = datetime.datetime(*timestamp)
        else: # assume it's a datetime object
            m.timestamp = timestamp
    else: # just use current time
        m.timestamp = datetime.datetime.utcnow()

    m.payload = message_content

    return m

class TestException(Exception):
    """ custom Exception """

class SimpleTestNode(nodes.BaseNode):
    """ simle node, that can be used for unit testing
    """
    def __init__(self, *args, **kwargs):
        """
            :param delay: delay in milliseconds to simulate processing time
            :param logger: allows to pass a custom logger for tracing
        """
        self.delay = kwargs.pop('delay', 0)
        self.logger = kwargs.pop('logger', False) or logging.getLogger(__name__)
        super().__init__(*args, **kwargs)
        
        # Used to test if node is processed during test
        self.processed = False

    def process(self, msg):
        print("Process %s" % self.name)
        self.processed = True
        return msg

