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

To create a fresh project (partially implemented): ::

    pypeman startproject <project_name>

To start pypeman: ::

    pypeman start

To show a channel graph: ::

    pypeman graph

