#!/usr/bin/env python
import datetime
import uuid
import copy


class Message():
    def __init__(self, content_type='application/text', payload=None, meta=None):
        self.content_type = content_type
        self.timestamp = datetime.datetime.now()
        self.uuid = uuid.uuid4()
        self.payload = payload
        self.meta = meta

    def copy(self):
        return copy.deepcopy(self)
