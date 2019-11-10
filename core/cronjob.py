class CronJobBase:

    app = None
    errors = None

    def __init__(self, app):
        self.app = app


    async def validate(self):
        pass


    async def perform(self):
        raise NotImplementedError


    async def read(self, raw, protocol, validator=None):
        obj = protocol()
        obj.ParseFromString(raw.pop(0))
        if validator:
            clean, err = validator().load_proto(obj)
            if err:
                self.update_errors(err)
            return clean
        return obj


    def update_errors(self, errors):
        if not errors:
            return
        for field, messages in errors:
            if self.errors is None:
                self.errors = {}
            if not field in self.errors:
                self.errors[field] = []
            self.errors[field].extend(messages)
