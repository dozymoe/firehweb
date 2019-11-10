""" Majordomo Protocol Worker API

Implements the MDP/Worker spec at https://rfc.zeromq.org/spec:7.

Author: Fahri Reza <dozymoe@gmail.com>

Based on Python example by Min RK <benjaminrk@gmail.com>
Based on Java example by Arkadiusz Orzechowski
"""
import asyncio
import logging
import struct
import time
import zmq

import aiozmq

from ..misc.collections import is_list_alike
from ..misc.url import connect_url_from_dict
from . import api as MDP


class MessageBrokerWorker():
    """ Majordomo Protocol worker.

    Implements the MDP/Worker spec at https://rfc.zeromq.org/spec:7
    """
    HEARTBEAT_LIVENESS = 3 # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500 # msecs

    # Event Callbacks
    # Your application should attach a callable here to received requests
    # from broker, the function should quit immediately (don't do processing
    # in the function itself)
    on_request = None

    broker_url = None
    socket_future = None # This is asyncio.Future of the actual socket
    service_name = None

    broker_expired_at = None # The status of broker has become unknown
    broker_lifetime = None

    heartbeat_task = None
    socket_task = None

    loop = None
    logger = None
    running = None

    def __init__(self, role, config, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.logger = logging.getLogger(type(self).__name__)
        self.running = True
        self.broker_lifetime = self.HEARTBEAT_INTERVAL *\
                self.HEARTBEAT_LIVENESS / 1000.0

        self.broker_url = connect_url_from_dict(config, prefix=role + '.',
                scheme='tcp', host='127.0.0.1', port=8002)

        self.service_name = struct.pack('B', config['service'])


    async def start(self):
        await self._reconnect()

        self.heartbeat_task = asyncio.ensure_future(
                self.heartbeat_task_worker(self.HEARTBEAT_INTERVAL),
                loop=self.loop)

        while self.running:
            socket = await self.socket_future
            self.socket_task = asyncio.ensure_future(socket.read(),
                    loop=self.loop)

            try:
                message = await self.socket_task
            except asyncio.CancelledError:
                continue
            except aiozmq.ZmqStreamClosed:
                # Heartbeat forcefully disconnected the socket.
                continue

            empty = message.pop(0)
            assert empty == b''

            broker_protocol = message.pop(0)
            assert broker_protocol == MDP.WORKER_V1

            broker_command = message.pop(0)

            if broker_command != MDP.WORKER_V1_HEARTBEAT:
                self.logger.debug('%s: received message.', self.service_name)

            if broker_command == MDP.WORKER_V1_REQUEST:
                # We should pop and save as many addresses as there are
                # up to a null part, but for now, just save one
                peer_id = message.pop(0)

                empty = message.pop(0) # empty first multipart from REQ
                assert empty == b''

                if callable(self.on_request):
                    await self.on_request(peer_id, message) # pylint:disable=not-callable

                self.touch()

            elif broker_command == MDP.WORKER_V1_HEARTBEAT:
                self.touch()

            elif broker_command == MDP.WORKER_V1_DISCONNECT:
                await self._reconnect()

            else:
                self.logger.error('%s: invalid message.', self.service_name)

        # wait all tasks
        await asyncio.gather(self.heartbeat_task)

        await self.send_to_broker(MDP.WORKER_V1_DISCONNECT, None, None,
                socket=socket)


    def stop(self):
        self.running = False
        if self.socket_task:
            self.socket_task.cancel()


    async def destroy(self):
        socket = await self.socket_future
        socket.close()


    async def _reconnect(self):
        """ Connect or reconnect to broker.
        """
        if self.socket_future:
            socket = await self.socket_future
            socket.close()

        self.socket_future = asyncio.Future(loop=self.loop)

        socket = await aiozmq.create_zmq_stream(zmq.DEALER,
                loop=self.loop)

        self.logger.info('%s: connecting to broker at %s', self.service_name,
                self.broker_url)

        await socket.transport.connect(self.broker_url)

        await self.send_to_broker(MDP.WORKER_V1_READY, self.service_name, None,
                socket=socket)

        self.touch()

        self.socket_future.set_result(socket)


    def touch(self):
        self.broker_expired_at = time.time() + self.broker_lifetime


    async def heartbeat_task_worker(self, interval): # msecs
        interval = interval / 1000.0

        while self.running:
            await asyncio.sleep(interval, loop=self.loop)
            if self.broker_expired_at < time.time():
                await self._reconnect()
            else:
                await self.send_to_broker(MDP.WORKER_V1_HEARTBEAT, None, None)


    async def send_to_broker(self, broker_command, option, messages,
            socket=None):

        """ Send message to broker.
        """
        if messages is None:
            messages = []

        # Stack routing and protocol envelopes to start of message
        if option is not None:
            messages = [option] + messages

        payload = [b'', MDP.WORKER_V1, broker_command] + messages

        if broker_command != MDP.WORKER_V1_HEARTBEAT:
            self.logger.debug('%s: sending %s to broker.', self.service_name,
                    broker_command)

        if socket is None:
            socket = await self.socket_future

        socket.write(payload)


    async def send(self, peer_id, messages):
        if messages is None:
            messages = []
        elif not is_list_alike(messages):
            messages = [messages]

        messages = [msg.SerializeToString() for msg in messages]
        payload = [peer_id, b''] + messages

        await self.send_to_broker(MDP.WORKER_V1_REPLY, None, payload)
