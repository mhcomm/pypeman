#!/usr/bin/env python

import json
import datetime
import asyncio

from aiohttp import web
from aiocron import crontab

# chain, jobs, receiver, cage, node, endpoint, payload,
# message, data, channel, pipeline, line

class Channel:
    def __init__(self):
        self._nodes = []

    def add(self, *args):
        for node in args:
            self._nodes.append(node)

    @asyncio.coroutine
    def process(self, message):
        result = message
        for node in self._nodes:
            result = yield from node(result)

        return result

@asyncio.coroutine
def log(message):
    print(message)
    return message


@asyncio.coroutine
def add1(message):
    message['test'] += 1
    return message


@asyncio.coroutine
def json_to_python(message):
    result = json.loads(message)
    return result


@asyncio.coroutine
def python_to_json(message):
    result = json.dumps(message)
    return result


class HttpEndpoint:
    app = None

    def __init__(self, loop, config, channel):
        if not HttpEndpoint.app:
            HttpEndpoint.app = web.Application(loop=loop)

        self.channel = channel
        HttpEndpoint.app.router.add_route(config['type'], config['url'], self.handle)

    @asyncio.coroutine
    def handle(self, request):
        content = yield from request.text()
        text = yield from self.channel.process(content)
        return web.Response(body=text.encode('utf-8'))


class TimeJob:
    def __init__(self, loop, config, channel):
        self.channel = channel
        crontab(config['cron'], func=self.handle, start=True)

    @asyncio.coroutine
    def handle(self):
        result = yield from self.channel.process(datetime.datetime.now())
        return result

route_types = {
    'http': HttpEndpoint,
    'time': TimeJob
}

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
    c = Channel()

    c.add(log, json_to_python, log, add1, add1, log, python_to_json)

    c2 = Channel()

    c2.add(log, log)

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