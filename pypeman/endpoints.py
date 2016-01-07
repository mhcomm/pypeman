import asyncio
from aiohttp import web
from pypeman.conf import settings

class HTTPEndpoint:
    def __init__(self, adress, port):
        self._app = None
        self.adress = adress
        self.port = port

    def add_route(self,*args, **kwargs):
        if not self._app:
            loop = asyncio.get_event_loop()
            self._app = web.Application(loop=loop)
        self._app.router.add_route(*args, **kwargs)

    @asyncio.coroutine
    def start(self):
        if self._app is not None:
            loop = asyncio.get_event_loop()
            srv = yield from loop.create_server(self._app.make_handler(), self.adress, self.port)
            print("Server started at http://{}:{}".format(self.adress, self.port))
        else:
            print("No HTTP route.")

http_endpoint = HTTPEndpoint(*settings.HTTP_ENDPOINT_CONFIG)

all = [http_endpoint]
