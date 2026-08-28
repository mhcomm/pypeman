Getting started
===============

Installation
------------

With pip ::

    pip install pypeman # or
    pip install pypeman[all] # To install with all optional dependencies

Basic usage
-----------

Create a fresh project with: ::

    pypeman-startproject <project_dirname>

Above command will create a new directory with a "settings.py" file containing
local configs and a "project.py" file with a channel example that
you can uncomment to test pypeman. Follow the commented instructions then execute: ::

    pypeman start # Stop it with Ctrl-C

Quick command overview
-----------------------

To get command help and more details about commands: ::

    pypeman --help

To create a fresh project: ::

    pypeman-startproject <project_name>

To start pypeman (stop it with Ctrl-C): ::

    pypeman start

To show a channel graph (as ascii art, graphviz dot or json): ::

    pypeman graph [--format {ascii,dot,json}]

To list the enabled plugins and the effective settings: ::

    pypeman listplugins
    pypeman printsettings

To launch a remote admin shell: ::

    pypeman shell


