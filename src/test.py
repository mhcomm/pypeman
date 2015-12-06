#!/usr/bin/env python

import asyncio
import aiohttp
import json
from aiohttp import web

# chain, jobs, receiver, cage, node, endpoint, payload,
# message, data, channel, pipeline, line

@asyncio.coroutine
def post_page(client, url):
    response = yield from client.post(url, data = b'{"test":1}')
    assert response.status == 200
    content = yield from response.read()
    return content


def main():
    print('start')
    loop = asyncio.get_event_loop()
    client = aiohttp.ClientSession(loop=loop)
    content = loop.run_until_complete(
        post_page(client, 'http://127.0.0.1:8080/toto'))
    print(content)
    content = loop.run_until_complete(
        post_page(client, 'http://127.0.0.1:8080/titi'))
    print(content)
    client.close()

if __name__ == '__main__':
    main()