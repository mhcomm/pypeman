import json
import logging
import ssl

import aiohttp

from aiohttp import web

from pypeman import endpoints, channels, nodes, message
from pypeman.errors import PypemanParamError


logger = logging.getLogger(__name__)


class HTTPEndpoint(endpoints.SocketEndpoint):
    """
    Endpoint to receive HTTP connection from outside.

    """
    def __init__(
            self,
            adress=None, address=None, port=None,  # obsolete params
            loop=None,
            http_args=None,
            host=None,
            sock=None,  # TODO: Why have sock and port if it's the same ?
            reuse_port=None,
            ):
        """
            :param http_args: dict to pass as **kwargs** to aiohttp.Application for example for
                `client_max_size`
            :param reuse_port: bool if true then the listening port specified in the url parameter)
                will be shared with other processes on same port
            :param host: string 'host:port' or ':port' or 'host'
            :param sock: host-string (same as host parameter)
                or socket-string ("unix:/sojet/file/path")
                or bound socket object
        """

        self.http_args = http_args or {}
        self.ssl_context = self.http_args.pop('ssl_context', None)
        self._app = None

        address = address or adress
        if address or port:
            raise PypemanParamError(
                "HTTPEndpoint Obsolete params ('address', 'port') ")

        if host and sock:
            raise PypemanParamError("There can only be one (parameter host or sock)")
        sock = sock or host or ''

        super().__init__(loop=loop, sock=sock, reuse_port=reuse_port)

    def add_route(self, *args, **kwargs):
        if self._app is None:
            self._app = web.Application(**self.http_args)

        self._app.router.add_route(*args, **kwargs)

    async def start(self):
        self.make_socket()
        if self._app is not None:
            srv = await self.loop.create_server(
                protocol_factory=self._app.make_handler(),
                sock=self.sock_obj,
                ssl=self.ssl_context,
            )
            message = "Server started at %r" % self.sock
            print(message)
            logger.info(message)
            return srv
        else:
            print("No HTTP route.")
            logger.warning("No HTTP route.")


class HttpChannel(channels.BaseChannel):
    """
    Channel that handles Http connection. The Http message is the message payload and some headers
    become metadata of message. Needs ``aiohttp`` python dependency to work.

    :param endpoint: HTTP endpoint used to get connections.
    :param method: Method filter.
    :param url: Only matching urls messages will be sent to this channel.
    :param encoding: Encoding of message. Default to 'utf-8'.

    """
    app = None

    def __init__(self, *args, endpoint=None, method='*', url='/', encoding=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.method = method
        self.url = url
        self.encoding = encoding
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.http_endpoint = endpoint

    async def start(self):
        first_start = self._first_start
        await super().start()
        if first_start:
            self.http_endpoint.add_route(self.method, self.url, self.handle_request)

    async def handle_request(self, request):
        content = await request.text()

        # extract match info from iohttp request
        meta = dict(request.match_info)

        # match infos will be overwritten if coinciding with known keywords like
        # 'url', 'method', . . .
        meta.update({
            'method': request.method,
            'url': str(request.url),
            'get_params': dict(request.query),
            })

        msg = message.Message(content_type='http_request', payload=content, meta=meta)
        try:
            result = await self.handle(msg)
            encoding = self.encoding or 'utf-8'
            return web.Response(body=result.payload.encode(encoding), status=result.meta.get('status', 200))

        except channels.Dropped:
            return web.Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as e:
            return web.Response(body=str(e).encode('utf-8'), status=503)


class HttpRequest(nodes.BaseNode):
    """
        Http request node
        :param url: url to send.
        :param method: 'get', 'put' or 'post', use meta['method'] if None, Default to 'get'.
        :param headers: headers for request, use meta.get('headers') if None.
        :param cookies: cookies for request, use meta.get('cookies') if None.
        :param auth: tuple or aiohttp.BasicAuth object.
        :param verify: verify ssl. Default True.
        :param params: get params in dict. List for multiple elements, ex :
                       {'param1': 'omega', param2: ['alpha', 'beta']}
        :param client_cert: tuple with .crt and .key path
        :param binary: bool, Get response content as bytes
    """

    def __init__(self, url, *args, method=None, headers=None, auth=None,
                 verify=True, params=None, client_cert=None, cookies=None,
                 binary=False, json=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.method = method
        self.headers = headers
        self.cookies = cookies
        self.auth = auth
        self.verify = verify
        self.params = params
        self.client_cert = client_cert
        self.url = self.url.replace('%(meta.', '%(')
        self.payload_in_url_dict = 'payload.' in self.url
        self.binary = binary
        self.json = json
        # TODO: create used payload keys for better perf of generate_request_url()

    def generate_request_url(self, msg):
        url_dict = msg.meta
        if self.payload_in_url_dict:
            url_dict = dict(url_dict)
            try:
                for key, val in msg.payload.items():
                    url_dict['payload.' + key] = val
            except AttributeError:
                self.channel.logger.exception(
                    "Payload must be a python dict if used to generate url. "
                    "This can be fixed using JsonToPython node before your "
                    "RequestNode")
                raise
        return self.url % url_dict

    async def handle_request(self, msg):
        """ generate url and handle request """
        url = self.generate_request_url(msg)
        loop = self.channel.loop
        conn = None
        ssl_context = None
        if self.client_cert:
            if self.verify:
                ssl_context = ssl.create_default_context()
            else:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            try:
                ssl_context.load_cert_chain(self.client_cert[0], self.client_cert[1])
            except FileNotFoundError:
                logger.error("loading certs %s failed", self.client_cert)
                raise
            conn = aiohttp.TCPConnector(ssl=ssl_context, loop=loop)
        else:
            conn = aiohttp.TCPConnector(ssl=self.verify, loop=loop)

        headers = nodes.choose_first_not_none(self.headers, msg.meta.get('headers'))
        cookies = nodes.choose_first_not_none(self.cookies, msg.meta.get('cookies'))
        method = nodes.choose_first_not_none(self.method, msg.meta.get('method'), 'get')
        params = nodes.choose_first_not_none(self.params, msg.meta.get('params'))

        get_params = None
        if params:
            get_params = []
            for key, param in params.items():
                if isinstance(param, list):
                    for value in param:
                        get_params.append((key, value))
                else:
                    get_params.append((key, param))

        if isinstance(self.auth, tuple):
            basic_auth = aiohttp.BasicAuth(self.auth[0], self.auth[1])
        else:
            basic_auth = self.auth

        data = None
        if method.lower() in ['put', 'post']:
            data = msg.payload
        async with aiohttp.ClientSession(connector=conn, cookies=cookies) as session:
            resp = await session.request(
                    method=method,
                    url=url,
                    auth=basic_auth,
                    headers=headers,
                    params=get_params,
                    data=data
                    )
            if self.binary:
                resp_content = await resp.read()
            else:
                resp_content = str(await resp.text())
            if self.json:
                try:
                    resp_content = json.loads(resp_content)
                except Exception as exc:
                    logger.error(
                        "cannot json parse response from url %s (response=%r)",
                        url, resp_content)
                    raise exc
            return resp_content

    async def process(self, msg):
        """ handles request """
        msg.payload = await self.handle_request(msg)
        return msg
