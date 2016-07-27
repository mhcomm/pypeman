import asyncio
import sys
import warnings

all = []

# used to share external dependencies
ext = {}

class BaseEndpoint:
    dependencies = [] # List of module requirements

    def __init__(self):
        all.append(self)

    def requirements(self):
        """ List dependencies of modules if any """
        return self.dependencies

    def import_modules(self):
        """ Use this method to import specific external modules listed in dependencies """
        pass


class HTTPEndpoint(BaseEndpoint):
    dependencies = ['aiohttp']

    def __init__(self, adress='127.0.0.1', port='8080'):
        super().__init__()
        self._app = None
        self.address = adress
        self.port = port

    def import_modules(self):
        if 'aiohttp_web' not in ext:
            from aiohttp import web

            ext['aiohttp_web'] = web

    def add_route(self,*args, **kwargs):
        if not self._app:
            loop = asyncio.get_event_loop()
            self._app = ext['aiohttp_web'].Application(loop=loop)
        # TODO route should be added later
        self._app.router.add_route(*args, **kwargs)

    @asyncio.coroutine
    def start(self):
        if self._app is not None:
            loop = asyncio.get_event_loop()
            srv = yield from loop.create_server(self._app.make_handler(), self.address, self.port)
            print("Server started at http://{}:{}".format(self.address, self.port))
            return srv
        else:
            print("No HTTP route.")

class MLLPProtocol(asyncio.Protocol):
    """
    Minimal Lower-Layer Protocol (MLLP) takes the form:
        <VT>[HL7 Message]<FS><CR>
    References:
    .. [1] http://www.hl7standards.com/blog/2007/05/02/hl7-mlp-minimum-layer-protocol-defined/
    .. [2] http://www.hl7standards.com/blog/2007/02/01/ack-message-original-mode-acknowledgement/
    """

    def __init__(self, handler):
        super().__init__()
        self._buffer = b''
        self.start_block = b'\x0b'  # <VT>, vertical tab
        self.end_block = b'\x1c'  # <FS>, file separator
        self.carriage_return = b'\x0d'  # <CR>, \r
        self.handler = handler

    def connection_made(self, transport):
        """
        Called when a connection is made.
        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        self.transport = transport

    def process_response(self, future):
        self.writeMessage(future.result())

    def data_received(self, data):
        """
        Called when some data is received.
        The argument is a bytes object.
        """
        # try to find a complete message(s) in the combined the buffer and data
        messages = (self._buffer + data).split(self.end_block)
        # whatever is in the last chunk is an uncompleted message, so put back
        # into the buffer
        self._buffer = messages.pop(-1)

        for raw_message in messages:
            # strip the rest of the MLLP shell from the HL7 message
            raw_message = raw_message.strip(self.start_block + self.carriage_return)

            print(repr(raw_message))

            # only pass messages with data
            if len(raw_message) > 0:
                result = asyncio.async(self.handler(raw_message))
                result.add_done_callback(self.process_response)


    def writeMessage(self, message):
        # convert back to a byte string
        # wrap message in payload container
        self.transport.write(self.start_block + message + self.end_block + self.carriage_return)


    def connection_lost(self, exc):
        """
        Called when the connection is lost or closed.
        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        super().connection_lost(exc)


class MLLPEndpoint(BaseEndpoint):
    dependencies = ['hl7']

    def __init__(self, address='127.0.0.1', port='2100', encoding='utf-8'):
        super().__init__()
        self.handlers = []
        self.address = address
        self.port = port

        if encoding != 'utf-8':
            warnings.warn("MLLPEndpoint 'encoding' parameters is deprecated", DeprecationWarning)
        self.encoding = encoding

    def import_modules(self):
        if 'hl7' not in ext:
            import hl7
            ext['hl7'] = hl7

    def set_handler(self, handler):
        self.handler = handler

    @asyncio.coroutine
    def start(self):
        if self.handler:
            loop = asyncio.get_event_loop()
            srv = yield from loop.create_server(lambda: MLLPProtocol(self.handler), self.address, self.port)
            print("MLLP server started at http://{}:{}".format(self.address, self.port))
            return srv
        else:
            print("No MLLP handlers.")


