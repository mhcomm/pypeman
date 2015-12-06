# pypeman

Minimalist but pragmatic ESB / ETL in python


# Installation

  python setup.py install # 'develop' while developping

# Usage

Create a "project" file in any folder containing for example:

  from pypeman import channels
  from pypeman import nodes
  
  c = channels.HttpChannel(method='*', url='/{name}')

  c.add(nodes.Log(), nodes.JsonToPython(), nodes.Log(), nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())
  
  
then execute:

  pypeman start