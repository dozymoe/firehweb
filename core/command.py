import logging

_logger = logging.getLogger(__name__)


class CommandBase:

    COMMAND = None

    app = None
    msgbus = None
    peer_id = None
    errors = None

    def __init__(self, peer_id, msgbus, app):
        assert self.COMMAND is not None

        self.app = app
        self.msgbus = msgbus
        self.peer_id = peer_id


    async def validate(self, raw, request):
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
        for field, messages in errors.items():
            if self.errors is None:
                self.errors = {}
            if not field in self.errors:
                self.errors[field] = []
            self.errors[field].extend(messages)


    async def reply(self, message):
        peer_id = self.peer_id
        try:
            await self.msgbus.send(peer_id, message)
        except: #pylint:disable=bare-except
            _logger.exception("Problem replying to client: %s.", peer_id)
