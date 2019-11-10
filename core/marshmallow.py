import logging
from marshmallow import Schema as BaseSchema, ValidationError
from marshmallow import fields # pylint:disable=unused-import

from .. import protocols as proto

_logger = logging.getLogger(__name__)


class Schema(BaseSchema):
    def load_proto(self, obj):
        try:
            data = self.load(proto.to_dict(obj))
            error = None
        except ValidationError as err:
            data = err.valid_data
            error = err.messages

        return (proto.from_dict(data, type(obj)), error)
