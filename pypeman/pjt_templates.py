import os

def new_project(dirname):
    os.makedirs(dirname)
    mk_pjt_files(dirname)

def mk_pjt_files(dirname):

    files_to_write = {
        'project.py': PJT_TEMPLATE,
        'settings.py': SETTINGS_TEMPLATE,
        'dist_settings.py': DIST_SETTINGS_TEMPLATE,
        'tests.py': TEST_TEMPLATE,
    }

    ctx = dict()
    for fname, template in files_to_write.items():
        full_name = os.path.join(dirname, fname)

        with open(full_name, 'w') as fout:
            fout.write(template % ctx)


PJT_TEMPLATE = """\
# Here you can add any special system path for your project
# import sys
# sys.path.insert(your_path)

from pypeman import channels
from pypeman import nodes
from pypeman import endpoints

from pypeman.conf import settings

# Create or update a "./test*.txt" file in you project dir. This file must contains a valid json dict.
# For example with shell command : echo "{}" > ./test.txt
# Uncomment next lines to enable FileWatcher example

#class CustomNode(nodes.BaseNode):
#    def process(self, msg):
#        msg.payload['new_key'] = "new_value"
#        return msg
#
#filewatcher = channels.FileWatcherChannel(name="jsontojson", path='./', regex="test.*\.txt")
#
#filewatcher.add(nodes.Log(), nodes.JsonToPython(), CustomNode(), nodes.PythonToJson(), nodes.Log())
#

# Example for testing purpose
#filewatcher2 = channels.FileWatcherChannel(name="great", path='./', regex="empty.txt")
#
#filewatcher2.add(nodes.Log(name="first"), nodes.Log(name="second"))

"""

# TODO Unused but should be done
"""# HTTP channel example (Remember to install required dependency before running it. Use "$pypeman requirements" command.

# If you use Http channel, at least one end point MUST be specified
# http = endpoints.HTTPEndpoint(address='0.0.0.0', port='8080')"""

SETTINGS_TEMPLATE = """\
# This is the local settings File For pypeman
# This file should contain all settings, that should NOT be added to your version control
# System.
# This file should contain settings like log settings, urls, usernames, passwords

from dist_settings import *

# Here you can configure the logging for Pypeman
# the framework will call logging.config.dictConfig(settings.LOGGING)
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'basic' : {
            'format': '%%(levelname)-8s %%(asctime)s %%(module)s %%(message)s',
        },
    },
    'handlers' : {
        'console': {
            'level' : 'INFO',
            'class' : 'logging.StreamHandler',
            'formatter' : 'basic',
        },
    },
    'loggers' : {
        '': { # The root logger
            'level': 'INFO',
            'handlers' : [ 'console' ],
        },
    },
}

"""

DIST_SETTINGS_TEMPLATE = """\
# This is the centralized settings File For pypeman
# This file should contain all defaults settings, that should be added to your version control
# System

# At the moment we really don't have anything useful project wide.
# Let's think about a good example

import os

# settings var pointing to project's directory
PJT_DIR = os.path.realpath(os.path.dirname(__file__))

"""

TEST_TEMPLATE = """\
from pypeman.test import PypeTestCase
from pypeman.message import Message

class MyChanTest(PypeTestCase):

    def test1_great_channel(self):
        \"\"\" Test example \"\"\"
        chan = self.get_channel("great")

        msg = Message(payload="X")
        msg_a = Message(payload="A")

        def append_b(msg):
            msg.payload += "B"
            return msg

        chan.get_node("first").mock(input=msg_a)
        chan.get_node("second").mock(output=append_b)

        result = chan.handle_and_wait(msg)

        self.assertEqual(chan.get_node("first").processed, 1)
        self.assertEqual(chan.get_node("first").last_input().payload, "X")
        self.assertEqual(chan.get_node("second").last_input().payload, "A")
        self.assertEqual(result.payload, 'AB')

"""
