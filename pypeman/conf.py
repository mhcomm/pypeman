#!/usr/bin/env python

# # Copyright  : (C) 2014 by MHComm. All rights reserved
#
# Name       : conf.py
"""
   Summary    :  TBD...
"""
# from __future__ import absolute_import

__author__ = "jeremie"
__copyright__ = "(C) 2016 by MHComm. All rights reserved"
__email__ = "info@mhcomm.fr"

import sys
import importlib
import traceback
import os

class Settings():

    def __init__(self):
        self.__dict__['_settings_mod'] = None
        self.__dict__['SETTINGS_MODULE'] = 'settings'

    def _init_settings(self):
        try:
            # TODO way be not the best way to do ?
            sys.path.append(os.getcwd())
            self.__dict__['_settings_mod'] = importlib.import_module(self.SETTINGS_MODULE)
        except:
            print("Can't import 'settings' module !")
            print(traceback.format_exc())
            sys.exit(-1)

    def __getattr__(self, name):
         if name in self.__dict__:
             return self.__dict__[name]

         if not self.__dict__.get('_settings_mod'):
             self._init_settings()

         return getattr(self.__dict__['_settings_mod'], name)

    def __setattr__(self, name, value):
         if name in self.__dict__:
             self.__dict__[name] = value
         else:
             print(name,value)
             raise Exception("Settings are not editable !")


settings = Settings()

