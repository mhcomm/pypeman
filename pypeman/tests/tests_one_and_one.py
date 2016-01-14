# pypeman -- A command-line argument parser for Python
# Copyright (C) 2015-2016 by MHComm. See LICENSE for details

import unittest

class OneAndOneTests(unittest.TestCase):
    def test_01_one_and_one(self):
        """ one and one makes two """
        self.assertEqual(1 + 1, 2)
