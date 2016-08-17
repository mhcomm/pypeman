#!/usr/bin/env python

DEBUG = False   # bool. can be set by env var PYPEMAN_DEBUG (0|1|true|false) or pypeman cmd args
TESTING = False # bool. can be set by env var PYPEMAN_TESTING (0|1|true|false) pypeman cmd args

HTTP_ENDPOINT_CONFIG = ['0.0.0.0', '8080']

handlers = [ 'console' ]

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            #'format': '%(levelname)s %(asctime)s %(name)s %(module)s %(process)d %(thread)d %(message)s'
            'format': '%(levelname)s %(asctime)s %(name)s %(module)s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'verbose'
        }
    },

    'loggers': {
        # root loggers
        '': {
            'level': 'INFO',
            'handlers': handlers,
        },
    }
}
