# pypeman

Minimalist but pragmatic ESB / ETL in python


[![Build Status](https://travis-ci.org/mhcomm/pypeman.svg?branch=master)](https://travis-ci.org/mhcomm/pypeman)

# Installation

  python setup.py install # 'develop' while developping

# Usage

Create a "settings.py" file with config in it.

Create a "project.py" file in any folder containing for example:

  `from pypeman import endpoints
  from pypeman import channels
  from pypeman import nodes
  
  http = endpoints.HTTPEndpoint(adress='localhost', port='8080')
  
  c = channels.HttpChannel(endpoint=http, method='*', url='/{name}')
  c.add(nodes.Log())
  
  c2 = c.fork()
  
  c2.add(nodes.JsonToPython(), nodes.Add1(), nodes.Add1(), nodes.Log())
  
  c.add(nodes.Log(), nodes.JsonToPython(), nodes.Log(), nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())
  `
  
then execute:

  `pypeman start` # You can use the --reload option for auto-reloading on changes
  
# Commands

To create a fresh project (partially implemented):

  `pypeman startproject`
  
To show a channel graph:

  `pypeman graph`

To list optional dependencies:
 
  `pypeman requirements`
