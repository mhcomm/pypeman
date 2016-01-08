import asyncio
import re
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




"""
        self.sb = "\x0b"
        self.eb = "\x1c"
        self.cr = "\x0d"
        self.validator = re.compile(self.sb + "(([^\r]+\r)*([^\r]+\r?))" + self.eb + self.cr)
        self.handlers = self.server.handlers
        self.timeout = self.server.timeout

        StreamRequestHandler.setup(self)

    def handle(self):
        end_seq = "{}{}".format(self.eb, self.cr)
        try:
            line = self.request.recv(3)
        except socket.timeout:
            self.request.close()
            return

        if line[0] != self.sb:  # First MLLP char
            self.request.close()
            return

        while line[-2:] != end_seq:
            try:
                char = self.rfile.read(1)
                if not char:
                    break
                line += char
            except socket.timeout:
                self.request.close()
                return

        message = self._extract_hl7_message(line)
        if message is not None:
            try:
                response = self._route_message(message)
            except Exception:
                self.request.close()
            else:
                # encode the response
                self.wfile.write(response)
        self.request.close()

"""



class MLLPProtocol(asyncio.Protocol):

    def __init__(self):
        super().__init__()
        self._buffer = ''
        self.start_block = '\x0b'  # <VT>, vertical tab
        self.end_block = '\x1c'  # <FS>, file separator
        self.carriage_return = '\x0d'  # <CR>, \r

        """self.sb = "\x0b"
        self.eb = "\x1c"
        self.cr = "\x0d"
        self.validator = re.compile(self.sb + "(([^\r]+\r)*([^\r]+\r?))" + self.eb + self.cr)"""

    def connection_made(self, transport):
        """
        Called when a connection is made.
        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        print("Connection received!")
        self.transport = transport

    def data_received(self, data):
        """
        Called when some data is received.
        The argument is a bytes object.
        """
        print(data)

        # success callback
        def onSuccess(message):
            self.writeMessage(message)

        # try to find a complete message(s) in the combined the buffer and data
        messages = (self._buffer + data).split(self.end_block)
        # whatever is in the last chunk is an uncompleted message, so put back
        # into the buffer
        self._buffer = messages.pop(-1)

        for raw_message in messages:
            # strip the rest of the MLLP shell from the HL7 message
            raw_message = raw_message.strip(self.start_block + self.carriage_return)

            # only pass messages with data
            if len(raw_message) > 0:
                # convert into unicode, parseMessage expects decoded string
                #if isinstance(value, str):
                #    return value.decode(self.encoding, self.encoding_errors)
                #return unicode(value)
                #raw_message = self.factory.decode(raw_message)
                raw_message = raw_message.decode('utf-8')

                message_container = self.factory.parseMessage(raw_message)

                # error callback (defined here, since error depends on
                # current message).  rejects the message
                def onError(err):
                    reject = message_container.err(err)
                    self.writeMessage(reject)
                    return err

                # have the factory create a deferred and pass the message
                # to the approriate IHL7Receiver instance
                d = self.factory.handleMessage(message_container)

    def writeMessage(self, message):
        if message is None:
            return
        # convert back to a byte string
        message = self.factory.encode(message)
        # wrap message in payload container
        self.transport.write(
            self.start_block + message + self.end_block + self.carriage_return
        )


        """end_seq = "{}{}".format(self.eb, self.cr)
        try:
            line = self.request.recv(3)
        except socket.timeout:
            self.request.close()
            return

        if line[0] != self.sb:  # First MLLP char
            self.request.close()
            return

        while line[-2:] != end_seq:
            try:
                char = self.rfile.read(1)
                if not char:
                    break
                line += char
            except socket.timeout:
                self.request.close()
                return

        message = self._extract_hl7_message(line)
        if message is not None:
            try:
                response = self._route_message(message)
            except Exception:
                self.request.close()
            else:
                # encode the response
                self.wfile.write(response)
        self.request.close()"""

        #self.transport.write(b'echo:')
        #self.transport.write(data)

    def connection_lost(self, exc):
        """
        Called when the connection is lost or closed.
        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        print("Connection lost! Closing server...")
        server.close()


class MLLPEndpoint:
    def __init__(self, adress, port):
        self._app = None
        self.adress = adress
        self.port = port

    def add_listener(self, *args, **kwargs):
        self._app = True
        if not self._app:
            loop = asyncio.get_event_loop()
            self._app = web.Application(loop=loop)

        self._app.router.add_route(*args, **kwargs)

    @asyncio.coroutine
    def start(self):

        loop = asyncio.get_event_loop()
        server = loop.run_until_complete(loop.create_server(MLLPProtocol, self.adress, self.port))
        loop.run_until_complete(server.wait_closed())
        print("Server started at http://{}:{}".format(self.adress, self.port))


mllp_endpoint = MLLPEndpoint('0.0.0.0', 8081)
http_endpoint = HTTPEndpoint(*settings.HTTP_ENDPOINT_CONFIG)

all = [http_endpoint]
