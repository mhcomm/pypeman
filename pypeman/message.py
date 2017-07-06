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

DATE_FORMAT = '%Y%m%d_%H%M'

class Message():
    # TODO : add_ctx and delete_ctx

    PENDING = "pending"
    PROCESSING = "processing"
    ERROR = "error"
    REJECTED = "rejected"
    PROCESSED = "processed"

    def __init__(self, content_type='application/text', payload=None, meta=None):
        self.content_type = content_type
        self.timestamp = datetime.datetime.now()
        self.uuid = uuid.uuid4()

        self.payload = payload

        if meta is None:
            meta = {}
        self.meta = meta

        self.ctx = {}

    def copy(self):
        """
        Copy the message. Useful for channel fork purpose.
        :return:
        """
        return copy.deepcopy(self)

    def renew(self):
        """
        Copy the message. Useful for channel fork purpose.
        :return:
        """
        msg = self.copy()

        msg.uuid = uuid.uuid4()
        msg.timestamp = datetime.datetime.now()
        return msg

    def add_context(self, key, msg):
        """ Add a msg to the current message context with key `key` """
        self.ctx[key] = dict(
            meta=dict(msg.meta),
            payload=copy.deepcopy(msg.payload),
        )

    def to_dict(self):
        """
        Convert a message object to a dict.
        :return: A dict with an equivalent of message
        """
        result = {}
        result['timestamp'] = self.timestamp.strftime(DATE_FORMAT)
        result['uuid'] = self.uuid.hex
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
        Create json data from current message.

        :return: a json string equivalent for message.
        """
        return json.dumps(self.to_dict())

    @staticmethod
    def from_dict(data):
        """
        Convert the input dict previously converted with `.as_dict()` method in Message object.
        :param data: The input dict
        :return: A message object
        """
        result = Message()
        result.timestamp = datetime.datetime.strptime(data['timestamp'], DATE_FORMAT)
        result.uuid = UUID(data['uuid'])
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
        Create a message from previously saved json data.

        :param data: Data to read message from.
        :return: a new message instance created from json data.
        """
        msg = Message.from_dict(json.loads(data))
        return msg


    def log(self, logger=default_logger, log_level=logging.DEBUG, payload=True, meta=True, context=False):
        """
        Log a message.

        :param logger: Logger
        :param log_level: log level for all log.
        :param payload: whether log payload.
        :param meta: whether log meta.
        :param context: whether log context.
        :return:
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

    def __str__(self):
        return "<msg: %s>" % self.uuid
