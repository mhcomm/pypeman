"""
serializers, that might be helpful for pypeman projects
"""

import json


# attempt to create serializers allowing to encode objects for non
# Python applications.
class JsonableEncoder:
    """ Initial simple encoder for objects.
        returns either
        - the object itself if it can be json encoded
        - a slightly transformed object, that can be json encoded (To be implemented)
        - a dict containing two fields: type and repr
    """
    def encode(self, obj):
        typ = None
        try:
            rslt = json.dumps(obj)
            typ = "json"
            return jsonstr
        except TypeError:
            pass

        typ = "repr"
        rslt = repr(obj)
        
        return rslt
        
