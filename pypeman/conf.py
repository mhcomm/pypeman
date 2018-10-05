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
import pypeman.default_settings as default_settings
import logging
import logging.config

NOT_FOUND = object()  # sentinel object


class ConfigError(ImportError):
    """ custom exception """


class Settings():
    """ pypeman projects settings. Rather similar implementations to django.conf.settings """

    def __init__(self, module_name=None):
        self.__dict__['_settings_mod'] = None
        if module_name:
            self.__dict__['SETTINGS_MODULE'] = module_name
        else:
            self.__dict__['SETTINGS_MODULE'] = os.environ.get('PYPEMAN_SETTINGS_MODULE', 'settings')

    def init_settings(self):
        try:
            settings_module = self.__dict__['SETTINGS_MODULE']
            settings_mod = self.__dict__['_settings_mod'] = importlib.import_module(settings_module)
        except Exception:
            msg = "Can't import '%s' module !" % self.__dict__['SETTINGS_MODULE']
            print(msg, file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            raise ConfigError(msg)

        # populate entire dict with values. helpful e.g. for ipython tab completion
        default_vals = [(key, val) for (key, val) in default_settings.__dict__.items()
                        if 'A' <= key[0] <= 'Z']
        self.__dict__.update(default_vals)

        mod_vals = [(key, val) for (key, val) in settings_mod.__dict__.items()
                    if 'A' <= key[0] <= 'Z']
        self.__dict__.update(mod_vals)

        logging.config.dictConfig(self.__dict__['LOGGING'])

    def __getattr__(self, name):
        """ lazy getattr. first access imports and populates settings """
        if name in self.__dict__:
            return self.__dict__[name]

        if not self.__dict__['_settings_mod']:
            self.init_settings()

        return self.__dict__[name]

    def __setattr__(self, name, value):
        """ make sure nobody tries to modify settings manually """
        if name in self.__dict__:
            self.__dict__[name] = value
        else:
            print(name, value)
            raise Exception("Settings are not editable !")


settings = Settings()
