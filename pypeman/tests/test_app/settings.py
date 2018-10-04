from pypeman.default_settings import *  # noqa F403

handlers = LOGGING['handlers']  # noqa F405
loggers = LOGGING['loggers']  # noqa F405

handlers['debug'] = {
    'class': 'pypeman.helpers.logging.DebugLogHandler',
    'level': 'DEBUG',
    }

loggers['']['level'] = 'WARNING'
loggers['']['handlers'].append('debug')
