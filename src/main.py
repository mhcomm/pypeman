#!/usr/bin/env python


import datetime
import channels
import nodes



# chain, jobs, receiver, cage, node, endpoint, payload,
# message, data, channel, pipeline, line




'''route_types = {
    'http': HttpEndpoint,
    'time': TimeJob
}'''

@asyncio.coroutine
def init_routes(loop, routes):

    for route in routes:
        route_types[route[0]](loop, route[1], route[2])

    srv = yield from loop.create_server(HttpEndpoint.app.make_handler(),
                                        '127.0.0.1', 8080)
    print("Server started at http://127.0.0.1:8080")
    return srv



def main():
    print('start')
    c = channels.HttpChannel(type='*', url='/{name}')

    c.add(nodes.Log(), nodes.JsonToPython(), nodes.Log(), nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())

    c2 = channels.TimeChannel(cron='*/10 * * * *')

    c2.add(nodes.Log(), nodes.Log())

    routes = (
        ('http', {'type':'POST', 'url':'/{name}'}, c),
        ('time', {'cron':'*/10 * * * *'}, c2),
    )

    # Reference the event loop.
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_routes(loop, routes))

    try:
        loop.run_forever()

    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()