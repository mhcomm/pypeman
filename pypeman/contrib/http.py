import asyncio

from pypeman import endpoints, channels, nodes, message

from aiohttp import web

# For compatibility purpose
from asyncio import async as ensure_future

class HTTPEndpoint(endpoints.BaseEndpoint):

    def __init__(self, adress='127.0.0.1', port='8080', loop=None):
        super().__init__()
        self._app = None
        self.address = adress
        self.port = port
        self.loop = loop or asyncio.get_event_loop()

    def add_route(self,*args, **kwargs):
        if not self._app:
            self._app = web.Application(loop=self.loop)
        # TODO route should be added later
        self._app.router.add_route(*args, **kwargs)

    @asyncio.coroutine
    def start(self):
        if self._app is not None:
            srv = yield from self.loop.create_server(self._app.make_handler(), self.address, self.port)
            print("Server started at http://{}:{}".format(self.address, self.port))
            return srv
        else:
            print("No HTTP route.")

class HttpChannel(channels.BaseChannel):
    """ Channel that handle http messages.
    """
    dependencies = ['aiohttp']
    app = None

    def __init__(self, *args, endpoint=None, method='*', url='/', encoding=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.method = method
        self.url = url
        self.encoding = encoding
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.http_endpoint = endpoint

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        self.http_endpoint.add_route(self.method, self.url, self.handle_request)

    @asyncio.coroutine
    def handle_request(self, request):
        content = yield from request.text()
        msg = message.Message(content_type='http_request', payload=content, meta={'method': request.method})
        try:
            result = yield from self.handle(msg)
            encoding = self.encoding or 'utf-8'
            return web.Response(body=result.payload.encode(encoding), status=result.meta.get('status', 200))

        except channels.Dropped:
            return web.Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as e:
            return web.Response(body=str(e).encode('utf-8'), status=503)