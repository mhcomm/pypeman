# pypeman

Minimalist but pragmatic ESB / ETL in python


[![Build Status](https://travis-ci.org/mhcomm/pypeman.svg?branch=master)](https://travis-ci.org/mhcomm/pypeman)

# Installation

  python setup.py install # 'develop' while developping

# Usage

Create a "settings.py" file with config in it.

Create a "project.py" file in any folder containing for example:

  from pypeman import endpoints
  from pypeman import channels
  from pypeman import nodes
  
  http = endpoints.HTTPEndpoint(adress='localhost', port='8080')
  
  c = channels.HttpChannel(endpoint=http, method='*', url='/{name}')
  c.add(nodes.Log())
  
  c2 = c.fork()
  
  c2.add(nodes.JsonToPython(), nodes.Add1(), nodes.Add1(), nodes.Log())
  
  c.add(nodes.Log(), nodes.JsonToPython(), nodes.Log(), nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())
  
  
then execute:

  pypeman start # You can use --reload option for auto-reloading on changes
  
# Commands

For creating fresh project (non implemented):

  pypeman startproject
  
For showing channel graph:

  pypeman graph

For list optionnal dependencies:
 
  pypeman requirements