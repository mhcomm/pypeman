#!/usr/bin/env python

# # Copyright  : (C) 2014 by MHComm. All rights reserved
#
# Name       : conf.py
"""
   Summary    :  TBD...
"""
from __future__ import absolute_import

__author__ = "jeremie"
__copyright__ = "(C) 2016 by MHComm. All rights reserved"
__email__ = "info@mhcomm.fr"

import sys
import importlib
import traceback
import os
import logging
import logging.config
import pypeman.default_settings as default_settings


NOT_FOUND = object()
class Settings():

    def __init__(self):
        self.__dict__['_settings_mod'] = None
        self.__dict__['SETTINGS_MODULE'] = os.environ.get('PYPEMAN_SETTINGS_MODULE', 'settings')

    def _init_settings(self):
        try:
            # TODO way be not the best way to do ?
            sys.path.append(os.getcwd())
            settings_module = self.__dict__['SETTINGS_MODULE']
            import logging as lg ; l = lg.getLogger(__name__) ; 
            l.warning('m:%r:f %r', __name__, __file__)
            l.warning('sm:%r', settings_module)
            settings_mod = self.__dict__['_settings_mod'] = importlib.import_module(settings_module)
        except:
            print("Can't import 'settings' module !", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            sys.exit(-1)

        # populate entire dict with values. helpful e.g. for iupython tab completion
        default_vals = [ (key, val) for (key, val) in default_settings.__dict__.items()
                if 'A' <= key[0] <= 'Z']
        self.__dict__.update(default_vals)
        mod_vals = [ (key, val) for (key, val) in settings_mod.__dict__.items()
                if 'A' <= key[0] <= 'Z']
        self.__dict__.update(mod_vals)

        logging.config.dictConfig(self.__dict__['LOGGING'])

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]

        if not self.__dict__['_settings_mod']:
            self._init_settings()

        return self.__dict__[name]


    def __setattr__(self, name, value):
        """ make sure nobody tries to modify settings manually """
        if name in self.__dict__:
             self.__dict__[name] = value
        else:
             print(name,value)
             raise Exception("Settings are not editable !")


settings = Settings()
