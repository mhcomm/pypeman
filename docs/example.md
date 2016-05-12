# Examples


```python
 from pypeman import endpoints
 from pypeman import channels
 from pypeman import nodes

 http = endpoints.HTTPEndpoint(adress='localhost', port='8080')

 main = channels.HttpChannel(endpoint=http, method='*', url='/{name}')
 main.add(nodes.Log())

 alt = main.fork()

 alt.add(nodes.JsonToPython(), nodes.Add1(), nodes.Add1(), nodes.Log())

 main.add(nodes.Log(), nodes.JsonToPython(), nodes.Log())
 main.add(nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())
```