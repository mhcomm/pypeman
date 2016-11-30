#!/usr/bin/env python

# ############################################################################
# Name       : cli.py
"""
  Summary    :  simple ipython debug CLI with basic fallback

"""
# #############################################################################
from __future__ import absolute_import

__author__    = "Klaus Foerster"

# -----------------------------------------------------------------------------
#   Imports
# -----------------------------------------------------------------------------
import threading
import logging

import readline


# -----------------------------------------------------------------------------
#   Globals
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)


class CLI(object):
    """ simplistic CLI (ipython based or fallback), that can be run in a thread 
        or in the main thread.
    """
    input_func = input

    def __init__(self, options=None, namespace=None, quit_func=None):
        cls = self.__class__
        self._cli_thread = None
        self._options = options

        # create a name space if not passed to __init__ and ensure, that
        # a lock (named __lock) is added to the name space if not already provided
        if namespace is None:
            namespace = {}
        self.namespace = namespace

        # a function to be called when the CLI will be quit.
        # it can do some cleanup work if required
        self._quit_function = quit_func

    def set_quit_function(self, func):
        self._quit_function = func


    def run(self):
        """ allows to run an ipython shell with the CLI's context vars """
        namespace = self.namespace
        try:
            from IPython.terminal.embed import InteractiveShellEmbed
            use_ipython = True
            logger.debug("CLI using ipython")
        except ImportError:
            use_ipython = False
            logger.debug("CLI using basic fallback")
            
        if use_ipython:
            shell = InteractiveShellEmbed(user_ns=namespace)
            shell()

        else:
            self.mini_shell(namespace=namespace)

        if self._quit_function:
            self._quit_function(self)

    def mini_shell(self, namespace):
        """ Rather lousy Python shell for debugging.
            Just in case ipython is not installed or has the wrong version
        """
        while True:
            cmd_line = self.input_func('-->')
            upper_stripped = cmd_line.strip().upper()
            shall_quit = (upper_stripped == 'Q' or upper_stripped == 'QUIT')
            if shall_quit:
                break
            try:
                eval(compile(cmd_line, '<string>', 'single'), namespace) # pylint: disable=W0122,C0301
            except Exception as exc: # pylint: disable=W0703
                logger.error('ERROR: %r' % exc)

        print("END OF CLI")

    def run_as_thread(self, name='cli', daemon=True):
        """ start CLI as a thread.
            This is needed for some programs where other code MUST be started in the main thread
            (e.g. PyQT applications)
        """
        self._cli_thread = cli_thread = threading.Thread(target=self.run, name=name)
        cli_thread.daemon = daemon 
        cli_thread.start()

