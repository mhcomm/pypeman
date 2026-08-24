Remote Admin
===============

Getting started
---------------

Pypeman allows you to access a remote instance through a custom command
shell.

The remote admin server is provided by the `RemoteAdminPlugin` (enabled
by default) and starts with `pypeman start`. It is served as part of the
shared plugins web app: host and port are configured through
`settings.WEBAPP_PLUGINS_CONFIG`, and the remote admin URL prefix through
`settings.REMOTE_ADMIN_CONFIG["url"]` (by default
`http://localhost:8091/remoteadmin/...`).

Custom command shell
--------------------

The custom command shell has simple commands to ease administration for rapid tasks but with less
possibility.

To launch the remote shell, execute: ::

    pypeman shell [host] [port]

You can show command help this way: ::

    pypeman > help # For command list
    pypeman > help <command name> # For help on a specific command
