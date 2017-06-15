Examples
========

.. code-block:: python

    from pypeman import endpoints
    from pypeman import channels
    from pypeman import nodes

    http = endpoints.HTTPEndpoint(address='localhost', port='8080')

    main = channels.HttpChannel(endpoint=http, method='*', url='/{name}')
    main.append(nodes.Log())

    alt = main.fork()

    alt.append(nodes.JsonToPython(), nodes.Add1(), nodes.Add1(), nodes.Log())

    main.append(nodes.Log(), nodes.JsonToPython(), nodes.Log())
    main.append(nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())
