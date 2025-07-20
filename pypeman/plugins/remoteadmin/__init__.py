"""Provides :class:`RemoteAdminPlugin`.

It comes in two parts:
    * a web server;
    * a 'shell' command.

[.. settings ..]

The 'shell' basically forwards commands to the web server. The web
server provides remote administrative procedures.
"""

from .plugin import RemoteAdminPlugin

__all__ = ("RemoteAdminPlugin",)
