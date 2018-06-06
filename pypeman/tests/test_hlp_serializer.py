from pypeman.helpers.serializers import JsonableEncoder
from pypeman.helpers.serializers import B64PickleEncoder


class TstMessage:
    def __init__(self, payload, meta=None, ctx=None):
        self.payload = payload


class SampleObject:
    """ sample object to test pickling """
    def __init__(self, val=1):
        self.val = val

    def __eq__(self, other):
        return self.val == other.val

    def __repr__(self):
        return "SampleObj(%s)" % self.val


class TestSerializer:
    all_bytes = ''.join(chr(v) for v in range(256))

    # JsonableEncoder and B64PickleEncoder should be able to handle this
    basicvals = {
        "none": None,
        "bool": True,
        "int": 1,
        "float": 0.3,
        "asciistring": "hello",
        "utf_string": "h\xe9llo",
        "utf_bytes": b"h\xe9llo",
        "allbytes": all_bytes,
    } 

    # JsonableEncoder and B64PickleEncoder should be able to handle this
    structvals_1 = {
        "list": [0, "1", 0.1, None, True, False],
        "dict_simple": {
                "a": 1,
                1: "b",
                1.1: "c",
            },
        }

    # Only B64PickleEncoder
    structvals_2 = {
        "list_with_asciibytes": [0, "1", b"3"],
        "list_with_utfbytes": [0, "1", b"3'\xe9"],
        "list_with_anybytes": [0, "1", all_bytes],
        "dict": {"key1" : 1,
                 2: "val",
                 "ke\xe0": 1.3,
                },
        "object": SampleObject(),
        }

    def check_encode_decode(self, codec, name, val):
        print(codec, name)
        rslt = codec.decode(codec.encode(val))
        assert (name, val) == (name, rslt)
        print(repr(val)[:40], repr(rslt)[:40])
        assert (name, type(val)) == (name, type(rslt))

    def test_enc_dec_match_B64Pickle(self):
        cls = self.__class__
        for codec_cls in [JsonableEncoder, B64PickleEncoder]:
            codec = codec_cls()
            for name, val in sorted(cls.basicvals.items()):
                self.check_encode_decode(codec, name, val)

        for codec_cls in [JsonableEncoder, B64PickleEncoder]:
            codec = codec_cls()
            for name, val in sorted(cls.structvals_1.items()):
                self.check_encode_decode(codec, name, val)
    
        for codec_cls in [B64PickleEncoder]:
            codec = codec_cls()
            for name, val in sorted(cls.structvals_2.items()):
                self.check_encode_decode(codec, name, val)

