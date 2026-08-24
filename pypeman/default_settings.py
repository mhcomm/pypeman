"""Default configuration.

You will want to override/update some of these values in your
project's setting module. See :mod:`pypeman.conf` and
:obj:`pypeman.conf.settings`.
"""

DEBUG = False  # bool. user projects may read it (eg to configure their own debug mode)
TESTING = False  # bool. idem

PROJECT_MODULE = "project"  # name of module containing the project
# (SETTINGS_MODULE is not a default: it is owned by pypeman.conf,
# from the PYPEMAN_SETTINGS_MODULE environment variable)

# Shared web app for every plugin using BundledWebappPluginMixin
# (each plugin is mounted under its own URL prefix).
WEBAPP_PLUGINS_CONFIG = {
    "host": "localhost",
    "port": 8091,
}

REMOTE_ADMIN_CONFIG = {
    "host": "localhost",
    "port": "8091",
    # "ssl": None, # TODO: ? see what it was meant to be
    "url": "",  # must be set when behind a reverse proxy
}

# TODO: being deprecated, see RemoteAdminPlugin's constructor
REMOTE_ADMIN_WEBSOCKET_CONFIG = {
    "host": "localhost",
    "port": "8091",
    "ssl": None,
    "url": "",  # must be set when behind a reverse proxy
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

# Loaded in order; override in the project settings module with
# a full list (settings are immutable once loaded).
PLUGINS = [
    "pypeman.plugins.plugins.ListPluginsPlugin",
    "pypeman.plugins.graph.GraphPlugin",
    "pypeman.plugins.settings.PrintSettingsPlugin",
    "pypeman.plugins.remoteadmin.RemoteAdminPlugin",
]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(levelname)s %(asctime)s %(name)s %(module)s %(message)s",
        },
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        "": {
            "level": "INFO",
            "handlers": ["console"],
        },
    },
}
