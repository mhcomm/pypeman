import json
import logging
import ssl
import re

import aiohttp

from aiohttp import web

from pypeman import endpoints, channels, nodes, message
from pypeman.errors import PypemanParamError


logger = logging.getLogger(__name__)

# Regex to extract dynamic params from a string to permits complex search
# in nested dicts:
#
# Example:
# url = "toto/tutu/%(gigi.gogo)s/bla/%(rigo)r/toto"
# Yields 2 results: "gigi.gogo" and "rigo"
str_named_param_regex = re.compile(r"%\((?P<keyval>[^\)]*)\)[r|s|d|]")

# Regex used to split a string by not escaped "."
# example:
# "titi.toto.tutu" yields 3 results: "titi", "toto", "tutu"
# "titi.toto\.tutu" yields 2 results: "titi", "toto\.tutu" (the \ will be removed in code)
not_escaped_dot_regex = re.compile(r"(?<!\\)\.")


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
        """
        Adds a route to the http server.
        This is normally called when an http channel is added to this endpoint
        """
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
    Channel that handles Http requests to a given path of the url.
    The Http message is the message payload and some headers
    become metadata of message. Needs ``aiohttp`` python dependency to work.

    :param endpoint: HTTP endpoint the channel will be using. ( scheme://fqdn:port for example)
    :param method: Method filter.
    :param url: Only matching urls messages will be sent to this channel.
    :param encoding: Encoding of message. Default to 'utf-8'.
    :param add_headers: if True headers will be added to the message meta data

    """
    app = None

    def __init__(
        self,
        *args,
        endpoint=None,
        method='*',
        url='/',
        encoding=None,
        add_headers=False,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.add_headers = add_headers
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
        if self.add_headers:
            meta["headers"] = [(hdr, val) for (hdr, val) in request.headers.items()]

        msg = message.Message(content_type='http_request', payload=content, meta=meta)
        try:
            result = await self.handle(msg)
            encoding = self.encoding or 'utf-8'
            return web.Response(
                body=str(result.payload).encode(encoding),
                status=result.meta.get('status', 200),
                content_type=getattr(result, "content_type", None),
            )

        except channels.Dropped:
            return web.Response(body="Dropped".encode('utf-8'), status=200)
        except Exception as exc:
            logger.exception(
                "HttpChannel %s raise an error, cannot send 200 speed response, will return 503",
                str(self))
            return web.Response(body=str(exc).encode('utf-8'), status=503)


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
                       The key can be a function that takes a msg as input param
        :param client_cert: tuple with .crt and .key path
        :param binary: bool, Get response content as bytes
        :param send_as_json: bool, If the method is a PATCH/POST/PUT, send data as json
        :param json: bool, Parse Json response content
        # TODO maybe add an auto parser if for example Content-Type header is application/json
    """

    def __init__(self, url, *args, method=None, headers=None, auth=None,
                 verify=True, params=None, client_cert=None, cookies=None,
                 binary=False, json=False, send_as_json=False, old_url_parsing=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.method = method
        self.headers = headers
        self.cookies = cookies
        if isinstance(auth, tuple):
            auth = aiohttp.BasicAuth(*auth)
        self.auth = auth
        self.verify = verify
        self.params = params
        self.client_cert = client_cert
        self.url = self.url.replace('%(meta.', '%(')  # TODO: why ???
        self.payload_in_url_dict = 'payload.' in self.url  # TODO: should I remove ?
        self.params_in_url = str_named_param_regex.findall(self.url)
        self.binary = binary
        self.json = json
        self.send_as_json = send_as_json
        self.old_url_parsing = old_url_parsing
        # TODO: create used payload keys for better perf of generate_request_url()

    def generate_request_url(self, msg):
        request_url = self.url
        if self.old_url_parsing:
            url_dict = msg.meta
            if self.payload_in_url_dict:
                url_dict = dict(url_dict)
                try:
                    for key, val in msg.payload.items():
                        url_dict['payload.' + key] = val
                except AttributeError:
                    self.channel.logger.error(
                        "Payload must be a python dict if used to generate url. "
                        "This can be fixed using JsonToPython node before your "
                        "RequestNode")
                    raise
            try:
                request_url = self.url % url_dict
            except Exception as exc:
                logger.error("cannot create url %r with args %r", self.url, repr(url_dict))
                raise exc
        else:
            # New recursive params parsing
            if self.params_in_url:
                params = {}
                for param in self.params_in_url:
                    subparams = not_escaped_dot_regex.split(param)
                    if subparams[0] == "payload":
                        data = msg.payload
                        subparams.pop(0)
                    elif subparams[0] == "meta":
                        data = msg.meta
                        subparams.pop(0)
                    else:
                        data = msg.meta
                    for subparam in subparams:
                        subparam = subparam.replace(r"\.", ".")
                        data = data[subparam]
                    params[param] = data
                try:
                    request_url = self.url % params
                except Exception as exc:
                    logger.error("cannot create url %r with args %r", self.url, repr(params))
                    raise exc
        return request_url

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
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
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
            params = params.copy()
            get_params = []
            for key, param in params.items():
                if callable(param):
                    param = param(msg)
                if isinstance(param, list):
                    for value in param:
                        get_params.append((key, value))
                else:
                    get_params.append((key, param))

        basic_auth = self.auth

        data = None
        if method.lower() in ['put', 'post', 'patch']:
            data = msg.payload
        async with aiohttp.ClientSession(
            connector=conn,
            cookies=cookies,
            trust_env=True,  # respect e.g. http_proxy env vars
        ) as session:
            if self.send_as_json:
                resp = await session.request(
                        method=method,
                        url=url,
                        auth=basic_auth,
                        headers=headers,
                        params=get_params,
                        json=data
                        )
            else:
                resp = await session.request(
                        method=method,
                        url=url,
                        auth=basic_auth,
                        headers=headers,
                        params=get_params,
                        data=data
                        )
            if self.binary:
                setattr(resp, "content", await resp.read())
            else:
                setattr(resp, "content", str(await resp.text()))
        return resp

    async def process(self, msg):
        """ handles request """
        resp = await self.handle_request(msg)
        msg.meta["status_code"] = resp.status
        msg.meta["url"] = str(resp.url)
        resp_content = resp.content
        if self.json:
            try:
                resp_content = json.loads(resp_content)
            except Exception:
                logger.exception(
                    "cannot json parse response from url %s (response=%r)",
                    self.generate_request_url(msg), resp_content)
        msg.payload = resp_content
        return msg
