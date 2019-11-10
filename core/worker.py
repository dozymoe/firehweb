import logging

from firehweb import protocols as web_proto


class WorkerMain:

    def __init__(self, commands, msgbus, app):
        self.app = app
        self.commands = commands
        self.msgbus = msgbus
        self.logger = logging.getLogger(type(self).__name__)


    async def reply(self, peer_id, message):
        try:
            await self.msgbus.send(peer_id, message)
        except: #pylint:disable=bare-except
            self.logger.exception("Problem replying to client: %s.", peer_id)


    async def __call__(self, peer_id, raw): #pylint:disable=too-many-function-args
        request = web_proto.Request()
        request.ParseFromString(raw.pop(0))

        command = request.command
        self.logger.debug('%s received public command: %s.',
                self.app['config']['service'], command)

        for handler_class in self.commands:
            if command == handler_class.COMMAND:
                try:
                    handler = handler_class(peer_id, self.msgbus, self.app)
                    await handler.validate(raw, request)
                    if not handler.errors:
                        await handler.perform()
                    else:
                        response = web_proto.Response()
                        response.status =\
                                web_proto.Response.Status.BAD_REQUEST
                        for field, messages in handler.errors.items():
                            for msg in messages:
                                err = response.errors.add()
                                err.field = field
                                err.msg = msg
                        await self.reply(peer_id, response)
                except: #pylint:disable=bare-except
                    self.logger.exception("Error processing %s.", command)
                    response = web_proto.Response()
                    response.status = web_proto.Response.Status\
                            .INTERNAL_SERVER_ERROR
                    await self.reply(peer_id, response)
                break
        else:
            response = web_proto.Response()
            response.status = web_proto.Response.Status.METHOD_NOT_ALLOWED
            err = response.errors.add()
            err.field = 'command'
            err.msg = "Invalid command: %s." % command
            await self.reply(peer_id, response)
