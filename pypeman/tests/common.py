"""  Functionality, that can be used bys eveal test cases

"""

import time
import datetime
import logging

from pypeman import nodes

def generate_msg(timestamp=None):
    # Default message
    m = message.Message()
    if timestamp:
        if type(timestamp) == tuple:
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

