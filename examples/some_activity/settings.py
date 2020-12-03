# This is the local settings File For pypeman
# This file should contain all settings, that should NOT be added to your version control
# System.
# This file should contain settings like log settings, urls, usernames, passwords

from dist_settings import *  # noqa: F401, F403
from dist_settings import PLUGINS

from dist_settings import LOGGING


# Here you can configure the logging for Pypeman
# the framework will call logging.config.dictConfig(settings.LOGGING)
PLUGINS.extend([
    "pypeman.plugins.base.BasePlugin",
    ])

LOGGING["formatters"].update({
    'basic': {
        'format': '%(levelname)-8s %(asctime)s %(module)s %(message)s',
        },
    'full': {
        'format': '%(levelname)-8s %(asctime)s %(name)s:%(lineno)d'
        ' %(process)d %(threadName)s %(message)s',
        }
    })
LOGGING["handlers"].update({
    'asyncio_file': {
        'level': 'DEBUG',
        'class': 'logging.FileHandler',
        'formatter': 'full',
        'filename': 'asyncio.log',
        }
    })

LOGGING["loggers"].update({
    '': {  # The root logger
        'level': 'DEBUG',
        'handlers': ['console', "asyncio_file"],
        },
    "parso": {
        "level": "WARNING",
        'handlers': ['console', 'asyncio_file'],
        'propagate': False,
        },
    'asyncio': {
        'level': 'INFO',
        'handlers': ['console', 'asyncio_file'],
        'propagate': False,
        },
    'pypeman.channels': {
        'level': 'DEBUG',
        'handlers': ['console', 'asyncio_file'],
        'propagate': False,
        },
    })
