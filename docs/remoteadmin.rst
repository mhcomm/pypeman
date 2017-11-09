Remote Admin
===============

Getting started
---------------

Pypeman allow you to access remote instance two ways:

- With a python shell that give access to a client module.
- With a custom command shell

You need to add `--remote-admin` at pypeman startup to enable remote admin websocket.

Python shell
------------

Python shell allow you to programatically administrate a remote pypeman instance through
python command. You can start the python shell by executing: ::

    pypeman pyshell

It starts a ipython instance with a RemoteAdminClient instance named `client`. The client instance API is:

.. autoclass:: pypeman.remoteadmin.RemoteAdminClient
    :members:
    :noindex:

Custom command shell
--------------------

The custom command shell has simple commands to ease administration for rapid tasks but with less
possibility.

To launch remote shell, execute: ::

    pypeman shell

You can show command help this way: ::

    pypeman > help # For command list
    pypeman > help <command name> # For help on a specific command
