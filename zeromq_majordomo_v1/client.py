import asyncio
import logging
import struct
import zmq

import aiozmq
import async_timeout

from ..misc.collections import is_list_alike
from ..misc.url import connect_url_from_dict
from .. import protocols as web_proto
from . import api as MDP


class MessageBrokerClient():

    SOCKET_READ_TIMEOUT = 10000 # msecs

    broker_url = None
    socket = None

    read_timeout = None
    retries = 3

    socket_task = None

    loop = None
    logger = None
    running = None

    def __init__(self, role, config, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.logger = logging.getLogger(type(self).__name__)
        self.running = True
        self.read_timeout = self.SOCKET_READ_TIMEOUT / 1000.0

        self.broker_url = connect_url_from_dict(config, prefix=role + '.',
                scheme='tcp', host='127.0.0.1', port=8002)


    async def start(self):
        await self._reconnect()


    async def _reconnect(self):
        """ Connect or reconnect to broker.
        """
        if self.socket:
            self.socket.close()

        self.socket = await aiozmq.create_zmq_stream(zmq.REQ,
                loop=self.loop)

        self.logger.debug('Connecting to broker at %s', self.broker_url)
        await self.socket.transport.connect(self.broker_url)


    async def send_to_broker(self, messages, timeout=None,
            retries=None):
        """ Send request to broker and get reply by hook or crook.

        Takes ownership of request message and destroys it when sent.
        Returns the reply message or None if there was no reply.
        """
        if self.socket is None:
            await self._reconnect()

        service = messages[0]

        # use majordomo v1
        # see: https://rfc.zeromq.org/spec:7/MDP
        payload = [MDP.CLIENT_V1] + messages

        if retries is None:
            retries = self.retries
        while self.running and retries > 0:
            self.socket.write(payload)
            try:
                with async_timeout.timeout(timeout or self.read_timeout,
                        loop=self.loop):

                    self.socket_task = asyncio.ensure_future(
                            self.socket.read(), loop=self.loop)

                    message = await self.socket_task

                self.logger.debug('Received reply')

                broker_protocol = message.pop(0)
                assert broker_protocol == MDP.CLIENT_V1

                # broadcast message
                if service.startswith(b'b.'):
                    service = service[2:]

                broker_service = message.pop(0)
                assert broker_service == service

                return message
            except asyncio.TimeoutError:
                retries -= 1
                self.logger.warning('No reply, reconnecting..')
                await self._reconnect()
                self.logger.warning('After reconnecting..')
            except asyncio.CancelledError:
                retries -= 1


    async def send(self, service, messages, timeout=None, retries=None):
        self.logger.debug('Send request to service: %s', service)

        if messages is None:
            messages = []
        elif not is_list_alike(messages):
            messages = [messages]

        messages = [msg.SerializeToString() for msg in messages]
        payload = [struct.pack('B', service)] + messages

        raw = await self.send_to_broker(payload, timeout, retries)
        response = web_proto.Response()
        response.ParseFromString(raw.pop(0))
        return (response, raw)


    def stop(self):
        self.running = False
        if self.socket_task is not None:
            self.socket_task.cancel()


    async def destroy(self, *args):
        if self.socket_task is not None:
            try:
                await self.socket_task
            except asyncio.CancelledError:
                pass

        if self.socket:
            self.socket.close()
