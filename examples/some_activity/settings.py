# This is the local settings File For pypeman
# This file should contain all settings, that should NOT be added to your version control
# System.
# This file should contain settings like log settings, urls, usernames, passwords

from dist_settings import *  # noqa: F401, F403


# Here you can configure the logging for Pypeman
# the framework will call logging.config.dictConfig(settings.LOGGING)
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'basic': {
            'format': '%(levelname)-8s %(asctime)s %(module)s %(message)s',
        },
        'full': {
            'format': '%(levelname)-8s %(asctime)s %(name)s:%(lineno)d'
            ' %(process)d %(threadName)s %(message)s',
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
        },
        'asyncio_file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'full',
            'filename': 'asyncio.log',
        }
    },
    'loggers': {
        '': {  # The root logger
            'level': 'INFO',
            'handlers': ['console'],
        },
        'asyncio': {
            'level': 'DEBUG',
            'handlers': ['console', 'asyncio_file'],
        },
        'pypeman.channels': {
            'level': 'DEBUG',
            'handlers': ['console', 'asyncio_file'],
        },
    },
}
