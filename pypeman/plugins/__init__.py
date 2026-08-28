"""Home package for all the plugins bundled with pypeman.

Plugins are added/removed through :obj:`pypeman.conf.settings`.
For example the following lines could be put in the project user
settings module:

    from pypeman.conf import settings
    settings.PLUGINS.add("...")

Or specify the whole exact set/list of plugins you want:

    PLUGINS = [
        "...",
        "...",
    ]
"""
