from pypeman.default_settings import *

handlers = LOGGING['handlers']
loggers = LOGGING['loggers']

handlers['debug'] = {
    'class': 'pypeman.helpers.logging.DebugLogHandler',
    'level': 'DEBUG',
    }

loggers['']['level'] = 'WARNING'
