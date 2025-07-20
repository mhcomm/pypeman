"""Default configuration.

You will want to override/update some of these values in your
project's setting module. See :mod:`pypeman.conf`.
"""

from os import environ

# TODO: this likely does not mean anything anymore (or at least that's my hope)
DEBUG = False  # bool. can be set by env var PYPEMAN_DEBUG (0|1|true|false) or pypeman cmd args
TESTING = False  # bool. can be set by env var PYPEMAN_TESTING (0|1|true|false) pypeman cmd args

PROJECT_MODULE = "project"  # name of module containing the project
SETTINGS_MODULE = environ.get("PYPEMAN_SETTINGS_MODULE", "settings")  # name of the project settings module

# TODO: this is not used anymore
DEBUG_PARAMS = {
    "slow_callback_duration": 0.1,
}

REMOTE_ADMIN_CONFIG = {
    "host": "localhost",
    "port": "8091",
    # "ssl": None, # TODO: ? see what it was meant to be
    "url": None,  # must be set when behind a reverse proxy
}

# TODO: being deprecated, see RemoteAdminPlugin's constructor
REMOTE_ADMIN_WEBSOCKET_CONFIG = {
    "host": "localhost",
    "port": "8091",
    "ssl": None,
    "url": None,  # must be set when behind a reverse proxy
}
REMOTE_ADMIN_WEB_CONFIG = {
    "host": "localhost",
    "port": "8090",
    "ssl": None,
}

HTTP_ENDPOINT_CONFIG = ["0.0.0.0", "8080"]

PERSISTENCE_BACKEND = None
PERSISTENCE_CONFIG = {}

RETRY_STORE_PATH = None

PLUGINS = [
    "pypeman.plugins.plugins.ListPluginsPlugin",
    "pypeman.plugins.graph.GraphPlugin",
    "pypeman.plugins.settings.PrintSettingsPlugin",
    # i dont like the name 'startproject' cause it seems to me like that
    # would be an alias for 'start' (or the other way around, you get it)
    # TODO: "pypeman.plugins.project.StartProjectPlugin",

    #"pypeman.plugins.remoteadmin.RemoteAdminPlugin",
    # REM: deprecation warning, with removal 2025-11-30 in jsonrpcserver (use of pkg_resources)
]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {"format": "%(levelname)s %(asctime)s %(name)s %(module)s %(message)s"},
    },
    "handlers": {"console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "verbose"}},
    "loggers": {
        "": {
            "level": "INFO",
            "handlers": ["console"],
        },
        "jsonrpcclient": {
            "level": "WARNING",
            "propagate": False,
        },
        "jsonrpcserver": {
            "level": "WARNING",
            "propagate": False,
        },
    },
}
