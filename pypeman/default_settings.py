"""
Default configuration file.
This settings should be redifined.
"""

DEBUG = False    # bool. can be set by env var PYPEMAN_DEBUG (0|1|true|false) or pypeman cmd args
TESTING = False  # bool. can be set by env var PYPEMAN_TESTING (0|1|true|false) pypeman cmd args

PROJECT_MODULE = "project"  # name of module containing the project

DEBUG_PARAMS = dict(
    slow_callback_duration=0.1
)

REMOTE_ADMIN_WEBSOCKET_CONFIG = {
    'host': 'localhost',
    'port': '8091',
    'ssl': None,
    'url': None,  # must be set when behind a reverse proxy
}

REMOTE_ADMIN_WEB_CONFIG = {
    'host': 'localhost',
    'port': '8090',
    'ssl': None,
}

HTTP_ENDPOINT_CONFIG = ['0.0.0.0', '8080']

PERSISTENCE_BACKEND = None
PERSISTENCE_CONFIG = {}

PLUGINS = []

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(name)s %(module)s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        }
    },

    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['console'],
        },
        'jsonrpcclient': {
            'level': 'WARNING',
            'propagate': False,
        },
        'jsonrpcserver': {
            'level': 'WARNING',
            'propagate': False,
        }
    }
}
