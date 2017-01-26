import asyncio
import ssl
import warnings

from pypeman import endpoints, channels, nodes, message

import aiohttp
from aiohttp import web

class HTTPEndpoint(endpoints.BaseEndpoint):
    """
    Endpoint to receive HTTP connection from outside.
    """

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
    """
    Channel that handles Http connection. The Http message is the message payload and some headers
    become metadata of message. Needs ``aiohttp`` python dependency to work.

    :param endpoint: HTTP endpoint used to get connections.

    :param method: Method filter.

    :param url: Only matching urls messages will be sent to this channel.

    :param encoding: Encoding of message. Default to 'utf-8'.
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


class HttpRequest(nodes.BaseNode):
    """ Http request node """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = kwargs.pop('url')
        self.method = kwargs.pop('method', None)
        self.headers = kwargs.pop('headers', None)
        self.auth = kwargs.pop('auth', None)
        self.verify = kwargs.pop('verify', True)
        self.client_cert = kwargs.pop('client_cert', None)
        self.url = self.url.replace('%(meta.', '%(')
        self.payload_in_url_dict = 'payload.' in self.url
        # TODO: create used payload keys for better perf of generate_request_url()

    def generate_request_url(self, msg):
        url_dict = msg.meta
        if self.payload_in_url_dict:
            url_dict = dict(url_dict)
            try:
                for key, val in msg.payload.items():
                    url_dict['payload.' + key] = val
            except AttributeError:
                self.channel.logger.exception("Payload must be a python dict if used to generate url. This can be fixed using JsonToPython node before your RequestNode")
                raise
        return self.url % url_dict

    @asyncio.coroutine
    def handle_request(self, msg):
        """ generate url and handle request """
        url = self.generate_request_url(msg)

        conn=None
        ssl_context=None
        if self.client_cert:
            if self.verify:
                ssl_context = ssl.create_default_context()
            else:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            ssl_context.load_cert_chain(self.client_cert[0], self.client_cert[1])
            conn = aiohttp.TCPConnector(ssl_context=ssl_context)
        else:
            conn = aiohttp.TCPConnector(verify_ssl=self.verify)

        headers = self.headers
        if not headers:
            headers = msg.meta.get('headers')
        method=self.method
        if not method:
            method = msg.meta.get('method','get')
        with aiohttp.ClientSession(connector=conn) as session:
            resp = yield from session.request(method=method, url=url, auth=self.auth, headers=headers)
            resp_text = yield from resp.text()
            return str(resp_text)

    @asyncio.coroutine
    def process(self, msg):
        """ handles request """
        msg.payload = yield from self.handle_request(msg)
        return msg


class RequestNode(HttpRequest):
    def __init__(self, *args, **kwargs):
        warnings.warn("RequestNode node is deprecated. New name is 'HttpRequest' node", DeprecationWarning)
        super().__init__(*args, **kwargs)
