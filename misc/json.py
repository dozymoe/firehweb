from datetime import date, datetime
import json


class JsonSerializer:

    app = None

    def __init__(self, app):
        self.app = app


    def __call__(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return str(obj)


def to_json(obj, *args, app=None, **kwargs):
    return json.dumps(obj, *args, default=JsonSerializer(app), **kwargs)
