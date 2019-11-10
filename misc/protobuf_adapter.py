try:
    from collections.abc import Mapping, MutableMapping
except ImportError:
    from collections import Mapping, MutableMapping
from datetime import date, datetime
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp


class ProtoBufAdapter(MutableMapping):

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, key):
        data = getattr(self.obj, self.__keytransform__(key))
        if isinstance(data, Timestamp):
            return data.ToDatetime()
        if isinstance(data, Message):
            return dict(ProtoBufAdapter(data))
        return data

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __iter__(self):
        return iter({x.name:y for x, y in self.obj.ListFields()})

    def __len__(self):
        return len(self.obj.ListFields())

    def __keytransform__(self, key):
        return key


def to_dict(obj):
    return dict(ProtoBufAdapter(obj))


def from_dict(data, proto):
    def recursive_set(prop, d):
        for key, value in d.items():
            if isinstance(value, (date, datetime)):
                getattr(prop, key).FromDatetime(value)
            elif isinstance(value, Mapping):
                recursive_set(getattr(prop, key), value)
            else:
                setattr(prop, key, value)

    obj = proto()
    recursive_set(obj, data)
    return obj
