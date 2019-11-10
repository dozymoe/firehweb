from .message_pb2 import Service, Request, Response

from ..misc.json import to_json as json_dump
from ..misc.protobuf_adapter import from_dict, to_dict


def get_errors(response):
    msgtypes = Response.Message.Type
    errors = {}

    for error in response.errors:
        if error.type == msgtypes.ERROR:
            type_ = 'error'
        elif error.type == msgtypes.WARNING:
            type_ = 'warning'
        elif error.type == msgtypes.INFO:
            type_ = 'info'
        elif error.type == msgtypes.DEBUG:
            type_ = 'debug'
        if type_ not in errors:
            errors[type_] = {}
        if error.field not in errors[type_]:
            errors[type_][error.field] = []
        errors[type_][error.field].append(error.msg)

    return errors


def to_json(obj, *args, app=None, **kwargs):
    return json_dump(to_dict(obj), *args, app=app, **kwargs)
