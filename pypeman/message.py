#!/usr/bin/env python
import datetime
import uuid
import copy
from uuid import UUID
import pickle
import json
import base64
import logging

default_logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


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

    def to_dict(self, encode_payload=True):
        """
        Convert the current message object to a dict.
        Payload is pickled and b64 encoded if encode_payload not set to False

        :return: A dict with an equivalent of message
        """
        result = {}
        result['timestamp'] = self.timestamp.strftime(DATE_FORMAT)
        result['uuid'] = self.uuid
        if encode_payload:
            result['payload'] = base64.b64encode(pickle.dumps(self.payload)).decode('ascii')
        else:
            try:
                result['payload'] = str(self.payload)
            except Exception:
                default_logger.warning("Cannot convert to string payload %r, pickling it")
                result['payload'] = base64.b64encode(pickle.dumps(self.payload)).decode('ascii')
        result['meta'] = self.meta
        result['ctx'] = {}

        for k, ctx_msg in self.ctx.items():
            result['ctx'][k] = {}
            result['ctx'][k]['payload'] = base64.b64encode(pickle.dumps(ctx_msg['payload'])).decode('ascii')
            result['ctx'][k]['meta'] = dict(ctx_msg['meta'])

        return result

    def to_json(self):
        """
        Create json string for current message.

        :return: a json string equivalent for message.
        """
        return json.dumps(self.to_dict())

    @staticmethod
    def from_dict(data):
        """
        Convert the input dict previously converted with `.as_dict()` method in Message object.

        :param data: The input dict.
        :return: The message message object correponding to given data.
        """
        result = Message()
        result.timestamp = datetime.datetime.strptime(data['timestamp'], DATE_FORMAT)
        result.uuid = UUID(data['uuid']).hex
        result.payload = pickle.loads(base64.b64decode(data['payload'].encode('ascii')))
        result.meta = data['meta']

        for k, ctx_msg in data['ctx'].items():
            result.ctx[k] = {}
            result.ctx[k]['payload'] = pickle.loads(base64.b64decode(ctx_msg['payload'].encode('ascii')))
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
