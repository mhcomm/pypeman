import asyncio
import logging
import socket

logger = logging.getLogger(__name__)


all_endpoints = []


def reset_pypeman_endpoints():
    """
    clears book keeping of all endpoints

    Can be useful for unit testing.
    """
    all_endpoints.clear()


class BaseEndpoint:

    def __init__(self):
        all_endpoints.append(self)

    async def start(self):
        pass

    async def stop(self):
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

        self.sock = self.normalize_socket(sock, default_port=default_port)
        self.sock_obj = None

    @staticmethod
    def normalize_socket(sock, default_port="8080"):
        if isinstance(sock, str):
            if not sock.startswith('unix:'):
                if ':' not in sock:
                    sock += ':'
                host, port = sock.split(":")
                host = host or '127.0.0.1'
                port = port or default_port
                sock = host + ':' + port
        return sock

    @staticmethod
    def mk_socket(sock, reuse_port):
        """
            make, bind and return socket if string object is passed
            if not return sock as is
        """
        if isinstance(sock, str):
            logger.debug("trying to create socket %s", sock)
            if not sock.startswith('unix:'):
                try:
                    host, port = sock.split(":")
                    port = int(port)
                    if not host:
                        raise
                    bind_param = (host, port)
                except Exception:
                    logger.error('error on sock params in socket endpoint')
                    raise
                sock_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_obj.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                bind_param = sock.split(":", 1)[1]
                sock_obj = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if reuse_port:
                SO_REUSEPORT = 15
                sock_obj.setsockopt(socket.SOL_SOCKET, SO_REUSEPORT, 1)
            sock_obj.bind(bind_param)
            logger.debug("socket %s created and binded", sock)
        else:
            logger.debug("no socket to create, keeping socket %s", repr(sock))
            sock_obj = sock
        return sock_obj

    def make_socket(self):
        """
            make and bind socket if string object is passed
        """
        self.sock_obj = self.mk_socket(self.sock, self.reuse_port)

    async def stop(self):
        if self.sock_obj:
            self.sock_obj.close()
            self.sock_obj = None


from pypeman.helpers import lazyload  # noqa: E402

wrap = lazyload.Wrapper(__name__)

wrap.add_lazy('pypeman.contrib.http', 'HTTPEndpoint', ['aiohttp'])

wrap.add_lazy('pypeman.contrib.hl7', 'MLLPEndpoint', ['hl7'])
