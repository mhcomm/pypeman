# pypeman -- A command-line argument parser for Python
# Copyright (C) 2015-2016 by MHComm. See LICENSE for details

import os
import sys
import unittest
import subprocess
import logging

from pypeman.helpers.tempfile import mktempfname

logger = logging.getLogger(__name__)

CWD = os.path.join('pypeman', 'tests', 'test_app')


class BinPypemanTestCase(unittest.TestCase):
    """ tests for the bin/pypeman cli """

    def setUp(self):
        """ prep a test """
        pypeman = os.path.join(os.path.dirname(__file__), '..', '..', 'pypeman', 'commands.py')
        self.cmd = [sys.executable, pypeman]
        os.environ['PYTHONPATH'] = os.path.join(os.path.dirname(__file__), '..', '..')
        self.tempfiles = []

    def tearDown(self):
        """ cleanup potentially created temp files """
        for fname in self.tempfiles:
            if os.path.exists(fname):
                os.unlink(fname)

    def run_pypeman(self, cmd, cwd=None):
        """
        Runs a command, gathers output and captures exit code
        :return: return_code and data (bytestring of stdout / stderr)
        """
        out_fname = mktempfname()
        self.tempfiles.append(out_fname)

        if cwd is None:
            cwd = os.getcwd()

        # TODO: why not reading stdout / stderr directoy into a var ??
        # TODO: if not reason can be found change code to use no temp file
        # TODO: perhaps some issue with unicode / python 3 ???
        with open(out_fname, 'wb') as fout:
            proc = subprocess.Popen(cmd, stdout=fout, stderr=fout, cwd=cwd)
            proc.wait()
        ret_code = proc.returncode

        data = None
        if ret_code:
            with open(out_fname, 'rb') as fin:
                data = fin.read()

        self.assertEqual(ret_code, 0, "exit code %d when calling %r in %s: %s" %
                         (ret_code, cmd, cwd, data))

        return ret_code, data

    def test_01_can_call_pypeman(self):
        """ pypeman can be called without params """
        logger.info("FILE = %r / NAME = %r", __file__, __name__)
        cmd = self.cmd
        self.run_pypeman(cmd)
        self.run_pypeman(cmd, cwd=CWD)

    def test_02_can_call_help(self):
        """ option -h is working """
        logger.info("FILE = %r / NAME = %r", __file__, __name__)

        cmd = self.cmd + ['-h']
        self.run_pypeman(cmd)
        self.run_pypeman(cmd, cwd=CWD)

    def test_03_can_call_graph(self):
        """ subcommand graph is working """

        cmd = self.cmd + ['graph']
        self.run_pypeman(cmd, cwd=CWD)

    def test_04_can_call_test(self):
        """ subcommand test is working """

        cmd = self.cmd + ['test']
        self.run_pypeman(cmd, cwd=os.path.join(os.path.dirname(CWD), 'test_app_testing'))

# test_suite =  BinPypemanTestCase
