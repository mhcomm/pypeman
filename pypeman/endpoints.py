import asyncio
import logging
import socket

logger = logging.getLogger(__name__)


all = []


class BaseEndpoint:

    def __init__(self):
        all.append(self)

    async def start(self):
        pass


class SocketEndpoint(BaseEndpoint):
    def __init__(self, loop=None, sock=None, default_port='8080', reuse_port=None):
        """
            :param reuse_port: bool if true then the listening port specified in the url parameter)
                will be shared with other processes on same port
                no effect with bound socket object
            :param sock: string 'host:port'
                or socket-string ("unix:/sojet/file/path")
                or bound socket object
        """
        super().__init__()
        self.loop = loop or asyncio.get_event_loop()
        self.reuse_port = reuse_port

        if isinstance(sock, str):
            if not sock.startswith('unix:'):
                if ':' not in sock:
                    sock += ':'
                host, port = sock.split(":")
                host = host or '127.0.0.1'
                port = port or default_port
                sock = host + ':' + port

        self.sock = sock

    def make_socket(self):
        """
            make and bind socket if string object is passed
        """
        sock = self.sock
        if isinstance(sock, str):
            if not sock.startswith('unix:'):
                try:
                    host, port = sock.split(":")
                    port = int(port)
                    if not host:
                        raise
                    bind_param = (host, port)
                except Exception:
                    logger.exception('error on sock params in socket endpoint')
                    raise
                sock_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                bind_param = sock.split(":", 1)[1]
                sock_obj = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if self.reuse_port:
                SO_REUSEPORT = 15
                sock_obj.setsockopt(socket.SOL_SOCKET, SO_REUSEPORT, 1)
            sock_obj.bind(bind_param)
        else:
            sock_obj = sock

        self.sock_obj = sock_obj


from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.http', 'HTTPEndpoint', ['aiohttp'])

wrap.add_lazy('pypeman.contrib.hl7', 'MLLPEndpoint', ['hl7'])
