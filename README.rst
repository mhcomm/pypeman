Welcome to Pypeman
==================

Pypeman is a minimalist but pragmatic ESB / ETL / EAI in python.

.. image:: https://travis-ci.org/mhcomm/pypeman.svg?branch=master
    :target: https://travis-ci.org/mhcomm/pypeman

.. image:: https://badge.fury.io/py/pypeman.svg
    :target: https://badge.fury.io/py/pypeman

.. image:: https://codecov.io/gh/mhcomm/pypeman/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/mhcomm/pypeman

.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://raw.githubusercontent.com/mhcomm/pypeman/master/LICENSE

.. image:: https://img.shields.io/pypi/pyversions/pypeman.svg
    :target: http://pypeman.readthedocs.org/en/latest/

.. image:: https://img.shields.io/pypi/wheel/pypeman.svg
    :target: http://pypeman.readthedocs.org/en/latest/

.. image:: https://img.shields.io/pypi/status/pypeman.svg
    :target: http://pypeman.readthedocs.org/en/latest/

See `documentation <http://pypeman.readthedocs.org/en/latest/>`_ for more information.

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

    pypeman startproject <project_dirname>

Above command will create a new directory with a "settings.py" file containing
local configs and a "project.py" file with a channel example that
you can uncomment to test pypeman. Follow the commented instructions then execute: ::

    pypeman start # You can use the --reload option for auto-reloading on changes

Quick command overview
-----------------------

To get command help and more details about commands: ::

    pypeman --help

To create a fresh project (partially implemented): ::

    pypeman startproject <project_name>

To start pypeman: ::

    pypeman start

To show a channel graph: ::

    pypeman graph

