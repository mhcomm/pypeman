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

# Remote admin plugin specific configuration; host/port of the server
# come from WEBAPP_PLUGINS_CONFIG.
# (REMOTE_ADMIN_WEBSOCKET_CONFIG / REMOTE_ADMIN_WEB_CONFIG are
# deprecated and must not be defined here: only user settings may.)
REMOTE_ADMIN_CONFIG = {
    # URL prefix; empty = served at the root of the shared web app.
    # Set eg "/remoteadmin" to namespace it (behind a reverse proxy...).
    "url": "",
}

# Health plugin specific configuration; host/port of the server come
# from WEBAPP_PLUGINS_CONFIG. The url must not collide with another
# plugin of the shared web app (eg REMOTE_ADMIN_CONFIG["url"]).
HEALTH_CONFIG = {
    "url": "/health",
    # a channel error more recent than this many seconds makes the
    # overall status "degraded"; 0 disables the criterion
    "degraded_error_window": 300,
}

# Metrics plugin specific configuration; same remarks as HEALTH_CONFIG.
METRICS_CONFIG = {
    "url": "/metrics",
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
    "pypeman.plugins.proctime.ProcTimePlugin",
    "pypeman.plugins.health.HealthPlugin",
    "pypeman.plugins.metrics.MetricsPlugin",
]

# Entries of PLUGINS (same dotted paths) to deactivate; lets a project
# drop some default plugins without redefining the whole PLUGINS list.
# An entry not found in PLUGINS is an error (typo protection).
DISABLED_PLUGINS = []

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
