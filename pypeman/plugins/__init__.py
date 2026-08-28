"""Home package for all the plugins bundled with pypeman.

Plugins are added/removed through the `PLUGINS` setting (settings are
immutable once loaded, so override the whole list). For example in the
project user settings module:

    from pypeman.default_settings import PLUGINS

    PLUGINS = PLUGINS + [
        "myproject.plugins.MyPlugin",
    ]

Or specify the whole exact list of plugins you want:

    PLUGINS = [
        "...",
        "...",
    ]
"""
