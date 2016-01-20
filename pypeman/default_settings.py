#!/usr/bin/env python

HTTP_ENDPOINT_CONFIG = ['0.0.0.0', '8080']

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
            'level': 'WARNING',
            'handlers': ['console'],
            'propagate': False,
        },
    }
}
