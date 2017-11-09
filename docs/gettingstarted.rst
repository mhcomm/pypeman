Getting started
===============

Installation
------------

With pip ::

    pip install pypeman # or
    pip install pypeman[all] # To install with all optionnal dependencies

Basic usage
-----------

Create a fresh project with: ::

    pypeman startproject <project_dirname>

Above command will create a new directory with a "settings.py" file containing
local configs and a "project.py" file with a channel example that
you can uncomment to test pypeman. Follow commented instructions then execute: ::

    pypeman start # You can use the --reload option for auto-reloading on changes

Quick commands overview
-----------------------

To get command help and more details about commands: ::

    pypeman --help

To create a fresh project: ::

    pypeman startproject <project_name>

To start pypeman as daemon: ::

    pypeman start [--reload] [--remote-admin]

To stop pypeman: ::

    pypeman stop

To show a channel graph: ::

    pypeman graph

To launch a remote shell (only if remote-admin is activated): ::

    pypeman shell


