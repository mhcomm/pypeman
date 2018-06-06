"""
serializers, that might be helpful for pypeman projects
"""

import json
import logging
import pickle

from base64 import b64decode
from base64 import b64encode


logger = logging.getLogger(__name__)


# attempt to create serializers allowing to encode objects for non
# Python applications.
class JsonableEncoder:
    """ Initial simple encoder/decoder for objects.
        returns:  a dict with type and value
            type: 'asis' or 'b64', 'repr'
            value:
              - the object itself if it can be json encoded (basetype or
                struct of base types)
              - a b64 encoded string if obj is of type bytes and not ASCII
              (- a slightly transformed object, that can be json encoded
                    (To be implemented))
              - a repr string of the object (which can normally not be decoded)
    """
    def encode(self, obj):
        logger.debug("try to encode type %s (%r)", type(obj), obj)
        result = {
            'type': 'asis',
            'value': obj,
            'repr': '',
            }
        if isinstance(obj, bytes):
            try:
                result['type'] = "bytes"
                result['value'] = obj.decode('ascii')  # OR UTF-8?
                return result
            except UnicodeDecodeError:
                pass
            try:
                result['type'] = "utf8bytes"
                # TODO could merge asis and utf8
                result['value'] = obj.decode('utf-8')
                return result
            except UnicodeDecodeError:
                result['type'] = "b64"
                result['value'] = b64encode(obj).decode()
                result['repr'] = obj.decode('utf-8', 'ignore')
                return result
        try:
            # logger.info("try to dump %s", repr(obj))
            json_str = json.dumps(obj)  # noqa this line will except if not jsonable
            # logger.info("can be dumped as json %s", json_str)
            return result
        except TypeError:
            pass
        result['type'] = "repr"
        # TODO: might add a base64 pickle for the repr case
        result['value'] = None
        result['repr'] = repr(obj)
        return result

    def decode(self, encoded_obj):
        enc_type = encoded_obj['type']
        data = encoded_obj['value']
        if enc_type == 'asis':
            return data
        elif enc_type in ('bytes', 'utf8bytes'):
            return data.encode('utf-8')
        elif enc_type == 'b64':
            return b64decode(data.encode('ascii'))


class B64PickleEncoder:
    """ Initial simple encoder for objects.
            objects are encoded, such, that they can be stored / transfered as
            an ASCII string.

            drawback: the encoded object can only be decoded if the
                application handles pickle. This limits this decoding mostly
                to python3 applications.
    """

    def encode(self, obj):
        """ the encoding function
            :param obj: the object to be encoded
            returns: a b64 encoded string of the pickled bytes of the passed
                     object
        """
        return b64encode(pickle.dumps(obj)).decode('ascii')

    def decode(self, encoded_obj):
        """ the decoding function
            :param encoded_obj: the encoded_object to be decoded
        """
        return pickle.loads(b64decode(encoded_obj.encode('ascii')))
