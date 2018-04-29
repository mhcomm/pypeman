"""
serializers, that might be helpful for pypeman projects
"""

import json
import logging
import base64

logger = logging.getLogger(__name__)

# attempt to create serializers allowing to encode objects for non
# Python applications.
class JsonableEncoder:
    """ Initial simple encoder for objects.
        returns:  a dict with type and value
            type: 'asis' or 'b64', 'repr'
            value:
              - the object itself if it can be json encoded (basetype or
                struct of base types)
              - a b64 encoded string if obj is of type bytes and not ASCII
              (- a slightly transformed object, that can be json encoded
                    (To be implemented))
              - a repr string of the object
    """
    def encode(self, obj):
        logger.debug("try to encode type %s (%r)", type(obj), obj)
        typ = None
        result = {
            'type': 'asis',
            'value': obj,
            'repr': '',
            }
        if isinstance(obj, bytes):
            try:
                result['type'] = "bytes"
                result['value'] = obj.decode('ascii') # OR UTF-8?
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
                result['value'] = base64.b64encode(obj).decode()
                result['repr'] = obj.decode('utf-8', 'ignore')
                return result
        try:
            # logger.info("try to dump %s", repr(obj))
            json_str = json.dumps(obj) # this line will except if not jsonable
            # logger.info("can be dumped as json %s", json_str)
            return result
        except TypeError:
            pass
        result['type'] = "repr"
        # TODO: might add a base64 pickle for the repr case
        result['value'] = None
        result['repr'] = repr(obj)
        return result

