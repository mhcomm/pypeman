# pypeman -- A command-line argument parser for Python
# Copyright (C) 2015-2016 by MHComm. See LICENSE for details

import os
import sys
import unittest
import subprocess
import logging

import nose.tools

from pypeman.helpers.tempfile import mktempfname

logger = logging.getLogger(__name__)

class BinPypemanTestCase(unittest.TestCase):
    """ tests for the bin/pypeman cli """

    def setUp(self):
        pypeman = os.path.join(os.path.dirname(__file__), '..', '..', 'bin', 'pypeman')
        self.cmd = [ sys.executable, pypeman ] 
    
    def test_01_can_call_help(self):
        """ option -h is working """
        logger.warning("FILE = %r / NAME = %r", __file__, __name__)
        # disable next line for testing. till pypeman works with option '-h'
        os.environ['PYPEMAN_SETTINGS_MODULE'] = 'pypeman.tst_helpers.test_setting_1'
        cmd = self.cmd + [ '-h' ]
        out_fname = mktempfname()
        with open(out_fname, 'wb') as fout:
            proc = subprocess.Popen(cmd, stdout=fout, stderr=fout)
            proc.wait()
        ret_code = proc.returncode
        if ret_code:
            with open(out_fname, 'rb') as fin:
                data = fin.read()
            nose.tools.eq_(ret_code, 0, data)
        os.unlink(out_fname)

