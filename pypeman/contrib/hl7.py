import sys
import asyncio
import warnings

from pypeman import endpoints, channels, nodes, message

# For compatibility purpose
from asyncio import async as ensure_future

import hl7

class MLLPProtocol(asyncio.Protocol):
    """
    Minimal Lower-Layer Protocol (MLLP) takes the form:
        <VT>[HL7 Message]<FS><CR>
    References:
    .. [1] http://www.hl7standards.com/blog/2007/05/02/hl7-mlp-minimum-layer-protocol-defined/
    .. [2] http://www.hl7standards.com/blog/2007/02/01/ack-message-original-mode-acknowledgement/
    """

    def __init__(self, handler, loop=None):
        super().__init__()
        self._buffer = b''
        self.start_block = b'\x0b'  # <VT>, vertical tab
        self.end_block = b'\x1c'  # <FS>, file separator
        self.carriage_return = b'\x0d'  # <CR>, \r
        self.handler = handler
        self.loop = loop or asyncio.get_event_loop()

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

            # only pass messages with data
            if len(raw_message) > 0:
                result = ensure_future(self.handler(raw_message), loop=self.loop)
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


class MLLPEndpoint(endpoints.BaseEndpoint):

    def __init__(self, address='127.0.0.1', port='2100', encoding='utf-8', loop=None):
        super().__init__()
        self.handlers = []
        self.address = address
        self.port = port
        self.loop = loop or asyncio.get_event_loop()

        if encoding != 'utf-8':
            warnings.warn("MLLPEndpoint 'encoding' parameters is deprecated", DeprecationWarning)
        self.encoding = encoding


    def set_handler(self, handler):
        self.handler = handler

    @asyncio.coroutine
    def start(self):
        if self.handler:
            srv = yield from self.loop.create_server(lambda: MLLPProtocol(self.handler, loop=self.loop), self.address, self.port)
            print("MLLP server started at http://{}:{}".format(self.address, self.port))
            return srv
        else:
            print("No MLLP handlers.")


class MLLPChannel(channels.BaseChannel):

    def __init__(self, *args, endpoint=None, encoding='utf-8', **kwargs):
        super().__init__(*args, **kwargs)
        if endpoint is None:
            raise TypeError('Missing "endpoint" argument')
        self.mllp_endpoint = endpoint

        if encoding is None:
            encoding = sys.getdefaultencoding()
        self.encoding = encoding

    @asyncio.coroutine
    def start(self):
        yield from super().start()
        self.mllp_endpoint.set_handler(handler=self.handle_hl7_message)

    @asyncio.coroutine
    def handle_hl7_message(self, hl7_message):
        content = hl7_message.decode(self.encoding)
        msg = message.Message(content_type='text/hl7', payload=content, meta={})
        try:
            result = yield from self.handle(msg)
            return result.payload.encode(self.encoding)
        except channels.Dropped:
            ack = hl7.parse(content, encoding=self.encoding)
            return str(ack.create_ack('AA')).encode(self.encoding)
        except channels.Rejected:
            ack = hl7.parse(content, encoding=self.encoding)
            return str(ack.create_ack('AR')).encode(self.encoding)
        except Exception:
            ack = hl7.parse(content, encoding=self.encoding)
            return str(ack.create_ack('AE')).encode(self.encoding)


class HL7ToPython(nodes.BaseNode):
    """ Convert hl7 payload to python struct."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = hl7.parse(msg.payload)
        msg.content_type = 'application/python'
        return msg


class PythonToHL7(nodes.BaseNode):
    """ Convert python payload to HL7. Must be HL7 structure."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = str(msg.payload)
        msg.content_type = 'text/hl7'
        return msg
