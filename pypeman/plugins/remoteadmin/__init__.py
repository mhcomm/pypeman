"""Provides :class:`RemoteAdminPlugin`.

It comes in two parts:
    * a web server;
    * a 'shell' command.

The web server is part of the shared plugins web app: its host/port
come from `settings.WEBAPP_PLUGINS_CONFIG`, and the remote admin URL
prefix from `settings.REMOTE_ADMIN_CONFIG["url"]`.

The 'shell' basically forwards commands to the web server. The web
server provides remote administrative procedures.
"""

from pypeman.plugins.remoteadmin.plugin import RemoteAdminPlugin

__all__ = ("RemoteAdminPlugin",)
