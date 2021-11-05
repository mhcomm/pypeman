#!/usr/bin/env python
import datetime
import uuid
import copy
from uuid import UUID
import json
import logging

from pypeman.helpers.serializers import B64PickleEncoder

default_logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DEFAULT_ENCODER_CLS = B64PickleEncoder



class Message():
    """
        A message is the unity of informations exchanged between nodes of a
        channel.

        A message have following properties:

            :attribute payload: The message content.
            :attribute meta: The message metadata.
            :attribute timestamp: The creation date of message
            :attribute uuid: uuid to identify message
            :attribute content_type: Used ?
            :attribute ctx: Current context when you want to save a message for later use.

        :param payload: You can initialise the payload by setting this param.
        :param meta: Same as payload, you can initialise the meta by setting this param.

    """
    # TODO : add_ctx and delete_ctx

    PENDING = "pending"
    PROCESSING = "processing"
    ERROR = "error"
    REJECTED = "rejected"
    PROCESSED = "processed"

    def __init__(self, content_type='application/text', payload=None, meta=None):
        self.content_type = content_type
        self.timestamp = datetime.datetime.now()
        self.uuid = uuid.uuid4().hex

        self.payload = payload

        if meta is None:
            meta = {}
        self.meta = meta

        self.ctx = {}

    def timestamp_str(self):
        """ Return timestamp formated string """
        return self.timestamp.strftime(DATE_FORMAT)

    def copy(self):
        """
        Copy the message. Useful for channel forking purpose.

        :return: A copy of current message.
        """
        return copy.deepcopy(self)

    def renew(self):
        """
        Copy the message but update the `timestamp` and `uuid`.

        :return: A copy of current message with new `uuid` and `Timestamp`.
        """
        msg = self.copy()

        msg.uuid = uuid.uuid4().hex
        msg.timestamp = datetime.datetime.now()
        return msg

    def add_context(self, key, msg):
        """
        Add a msg to the `.ctx` property with specified key.

        :param key: Key to store message.
        :param msg: Message to store.
        """
        self.ctx[key] = dict(
            meta=dict(msg.meta),
            payload=copy.deepcopy(msg.payload),
        )

    def to_dict(self, payload_encoder=None):
        """
        Convert the current message object to a dict, that can be converted
            to a json.
        Therefore the payload and all the context is pickled and base64 encoded.

        Warning: the returned dict cannot be converted to json if meta contains
            objects, that cannot be dumped as json.

        :param encoder: Encoder object that will be used to encode the payload
            if set to None B64PickleEncoder() will be used, which just pickles an object
            and encodes with base64.

        :return: A dict with an equivalent of message
        """

        payload_encoder = (payload_encoder if payload_encoder
                           else DEFAULT_ENCODER_CLS())

        encode = payload_encoder.encode
        result = {}
        result['timestamp'] = self.timestamp.strftime(DATE_FORMAT)
        result['uuid'] = self.uuid
        result['payload'] = encode(self.payload)
        result['meta'] = self.meta
        result['ctx'] = {}

        for k, ctx_msg in self.ctx.items():
            result['ctx'][k] = {}
            result['ctx'][k]['payload'] = encode(ctx_msg['payload'])
            result['ctx'][k]['meta'] = dict(ctx_msg['meta'])

        return result

    def to_json(self, payload_encoder=None):
        """
        Create json string for current message.

        :param encoder: Encoder object that will be used to encode the payload.
            more info at documentation of to_dict()

        :return: a json string equivalent for message.
        """
        return json.dumps(self.to_dict(payload_encoder=payload_encoder))

    @staticmethod
    def from_dict(data, payload_encoder=None):
        """
        Converts an input dict previously converted with `.to_dict()` method
            to a Message object.

        :param data: The input dict.

        :param encoder: Encoder object that will be used to decode the payload
            if set to None B64PickleEncoder() will be used, which decodes an
                object that was pickled and then base64 encoded.

        :return: The message message object corresponding to given data.
        """
        payload_encoder = (payload_encoder if payload_encoder
                           else DEFAULT_ENCODER_CLS())

        decode = payload_encoder.decode

        result = Message()
        result.timestamp = datetime.datetime.strptime(data['timestamp'], DATE_FORMAT)
        result.uuid = UUID(data['uuid']).hex
        result.payload = decode(data['payload'])
        result.meta = data['meta']

        for k, ctx_msg in data['ctx'].items():
            result.ctx[k] = {}
            result.ctx[k]['payload'] = decode(ctx_msg['payload'])
            result.ctx[k]['meta'] = dict(ctx_msg['meta'])

        return result

    @staticmethod
    def from_json(data):
        """
        Create a message from previously saved json string.

        :param data: Data to read message from.
        :return: A new message instance created from json data.
        """
        msg = Message.from_dict(json.loads(data))
        return msg

    def log(self, logger=default_logger, log_level=logging.DEBUG, payload=True, meta=True, context=False):
        """
        Log a message.

        :param logger: Logger
        :param log_level: log level for all log.
        :param payload: Whether log payload.
        :param meta: Whether log meta.
        :param context: Whether log context.
        """

        if payload:
            logger.log(log_level, 'Payload: %r', self.payload)

        if meta:
            logger.log(log_level, 'Meta: %r', self.meta)

        if context and self.ctx:
            logger.log(log_level, 'Context for message ->')
            for key, msg in self.ctx.items():
                logger.log(log_level, '-- Key "%s" --', key)
                if payload:
                    logger.log(log_level, 'Payload: %r', msg['payload'])

                if meta:
                    logger.log(log_level, 'Meta: %r', msg['meta'])

    def to_print(self, payload=True, meta=True, context=False):
        """
        Return a printable version of message.

        :param payload: Whether print payload.
        :param meta: Whether print meta.
        :param context: Whether print context.
        """

        result = "Message {msg.uuid}\nDate: {msg.timestamp}\n".format(msg=self)

        if payload:
            result += 'Payload: %r\n' % self.payload

        if meta:
            result += 'Meta: %r\n' % self.meta

        if context and self.ctx:
            result += 'Context for message ->\n'
            for key, msg in self.ctx.items():
                result += '-- Key "%s" --\n' % key
                if payload:
                    result += 'Payload: %r\n' % msg['payload']

                if meta:
                    result += 'Meta: %r\n' % msg['meta']

        return result

    def __str__(self):
        return "<msg: %s>" % self.uuid

    def __repr__(self):
        return "<msg: %s- id(%s)>" % (self.uuid, id(self))
