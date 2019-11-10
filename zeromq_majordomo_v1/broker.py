import asyncio
from binascii import hexlify
import logging
from operator import attrgetter
import time
import zmq

import aiozmq

from ..misc.collections import is_list_alike
from ..misc.url import listen_url_from_dict
from . import api as MDP


class Service():
    """ Majordomo service definition. """

    name = None
    workers = None

    def __init__(self, name):
        self.name = name
        self.workers = []


    def add_worker(self, worker):
        self.workers.append(worker)


    def remove_worker(self, worker):
        try:
            self.workers.remove(worker)
        except ValueError:
            pass


    def __repr__(self):
        return '<Service:%s>' % self.name


class Worker():
    """ Majordomo worker definition. """

    id_ = None
    address = None # address to route to
    service = None # owning service, if known
    expired_at = None  # expires at this point, unless heartbeat

    _lifetime = None # seconds

    def __init__(self, id_, address, lifetime): # msecs
        self.id_ = id_
        self.address = address
        self._lifetime = lifetime / 1000.0

        self.touch()


    def touch(self):
        self.expired_at = time.time() + self._lifetime


    def __repr__(self):
        return '<Worker:%s>' % self.id_


class MessageBrokerServer():
    """ Majordomo Protocol broker.

    A minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8
    """

    broadcast_service_prefix = None
    internal_service_prefix = None

    config = None
    role = None
    url = None
    socket = None

    services = None # known services
    requests = None # brokered messages
    idle_workers = None # idle workers
    all_workers = None # known workers

    heartbeat_task = None
    request_queue_task = None
    request_queue_sleep_task = None
    socket_task = None

    loop = None
    logger = None
    running = None

    def __init__(self, role, config, loop=None):
        self.role = role
        self.config = config
        self.loop = loop or asyncio.get_event_loop()

        self.services = {}
        self.requests = []
        self.all_workers = {}
        self.idle_workers = []
        self.logger = logging.getLogger(type(self).__name__)
        self.running = True

        self.url = listen_url_from_dict(config, prefix=role + '.',
                scheme='tcp', host='0.0.0.0', port=8002)

        self.broadcast_service_prefix = 'b.'.encode('ascii')
        self.internal_service_prefix = config.get(
                'application.internal_service_prefix',
                'mmi.').encode('ascii')


    async def start(self):
        self.logger.info('%s waiting for client in %s...', self.role, self.url)

        self.socket = await aiozmq.create_zmq_stream(zmq.ROUTER,
                loop=self.loop)

        await self.socket.transport.bind(self.url)

        self.heartbeat_task = asyncio.ensure_future(
                self.heartbeat_task_worker(
                    self.config.get('application.heartbeat.interval', 2500)),
                loop=self.loop)

        self.request_queue_task = asyncio.ensure_future(
                self.request_queue_task_worker(
                    self.config.get('application.process_requests_interval',
                        5000)),
                loop=self.loop)

        while self.running:
            self.socket_task = asyncio.ensure_future(self.socket.read(),
                    loop=self.loop)

            try:
                message = await self.socket_task
            except asyncio.CancelledError:
                continue

            peer_id = message.pop(0)

            empty = message.pop(0) # empty first multipart from REQ
            assert empty == b''

            broker_protocol = message.pop(0)

            if broker_protocol == MDP.CLIENT_V1:
                self.process_client(peer_id, message)

            elif broker_protocol == MDP.WORKER_V1:
                self.process_worker(peer_id, message)
                # Force queue worker to run
                self.request_queue_sleep_task.cancel()

            else:
                self.logger.error('Invalid message.')

        # wait all tasks
        await asyncio.gather(self.heartbeat_task, self.request_queue_task)
        self.socket.close()


    def stop(self, *args):
        self.running = False
        self.socket_task.cancel()


    async def destroy(self):
        """ Disconnect all workers, destroy socket.
        """
        while self.all_workers:
            worker = next(iter(self.all_workers.values()))
            self.remove_worker(worker, disconnect=True)


    def process_client(self, peer_id, message):
        broker_service = message.pop(0)

        self.logger.debug('Got client message for: %s.', broker_service)

        # Set reply return address to peer
        payload = [peer_id, b''] + message

        if broker_service.startswith(self.internal_service_prefix):
            self.dispatch_internal(broker_service, payload)
        elif broker_service.startswith(self.broadcast_service_prefix):
            self.dispatch_broadcast(
                    broker_service[len(self.broadcast_service_prefix):],
                    payload)
        else:
            self.dispatch(broker_service, payload)


    def process_worker(self, peer_id, message):
        broker_command = message.pop(0)

        if broker_command != MDP.WORKER_V1_HEARTBEAT:
            self.logger.debug('Got worker message for: %s.', broker_command)

        worker_id = hexlify(peer_id)
        worker_ready = worker_id in self.all_workers
        worker = self.require_worker(peer_id)

        if broker_command == MDP.WORKER_V1_READY:
            broker_service = message.pop(0)

            # Not first command in session or Reserved service_name
            if worker_ready or\
                    broker_service.startswith(self.internal_service_prefix) or\
                    broker_service.startswith(self.broadcast_service_prefix):

                self.remove_worker(worker, disconnect=True)
            else:
                # Attach worker to service and mark as idle
                worker.service = self.require_service(broker_service)
                # Also update expired_at for worker
                self.add_idle_worker(worker)

        elif broker_command == MDP.WORKER_V1_REPLY:
            self.logger.debug('Got reply from %s.', worker.service.name)
            if worker_ready:
                # Remove and save peer return envelope and insert the
                # protocol header and service name, then rewrap envelope
                client_id = message.pop(0)

                empty = message.pop(0)
                assert empty == b''

                payload = [client_id, b'', MDP.CLIENT_V1,
                        worker.service.name] + message

                self.socket.write(payload)
                # Also update expired_at for worker
                self.add_idle_worker(worker)
            else:
                self.remove_worker(worker, disconnect=True)

        elif broker_command == MDP.WORKER_V1_HEARTBEAT:
            if worker_ready:
                worker.touch()
                self.idle_workers.sort(key=attrgetter('expired_at'))
            else:
                self.remove_worker(worker, disconnect=True)

        elif broker_command == MDP.WORKER_V1_DISCONNECT:
            self.logger.info('Deleting worker: %s', worker.id_)
            self.remove_worker(worker, disconnect=False)

        else:
            self.logger.error('Invalid message.')


    def remove_worker(self, worker, disconnect):
        """ Deletes worker from all data structures, and delete workers.
        """
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, MDP.WORKER_V1_DISCONNECT, None, None)

        if worker.service is not None:
            worker.service.remove_worker(worker)

        try:
            self.all_workers.pop(worker.id_)
        except KeyError:
            pass

        try:
            self.idle_workers.remove(worker)
        except ValueError:
            pass


    def require_worker(self, peer_id):
        """ Find the worker (creates if necessary). """
        assert peer_id is not None

        worker_id = hexlify(peer_id)
        worker = self.all_workers.get(worker_id)

        if worker is None:
            worker = Worker(worker_id, peer_id,
                    self.config.get('application.heartbeat.liveness', 3) *\
                    self.config.get('application.heartbeat.interval', 2500))

            self.all_workers[worker_id] = worker
            self.logger.info('Registering new worker: %s', worker_id)

        return worker


    def require_service(self, name):
        assert name is not None
        service = self.services.get(name)

        if service is None:
            service = Service(name)
            self.services[name] = service

        return service


    def dispatch(self, service_name, message):
        """ Dispatch requests to waiting workers as possible. """
        # Queue message if any
        self.requests.append((service_name, message))


    def dispatch_broadcast(self, service_name, message):
        for worker in self.all_workers.values():
            if worker.service and worker.service.name == service_name:
                self.send_to_worker(worker, MDP.WORKER_V1_REQUEST, None,
                        message)


    def dispatch_internal(self, service_name, message):
        """ Handle internal service according to 8/MMI specification.
        """
        returncode = b'501'

        if service_name == 'mmi.service':
            broker_service = message[-1]
            returncode = b'200' if broker_service in self.services else b'404'

        message[-1] = returncode
        # Insert the protocol header and service name after the routing
        # envelope ([client, ''])
        payload = message[:2] + [MDP.CLIENT_V1, service_name] + message[2:]

        self.socket.write(payload)


    async def heartbeat_task_worker(self, interval): # msecs
        interval = interval / 1000.0

        while self.running:
            await asyncio.sleep(interval, loop=self.loop)
            self.prune_workers()
            self.send_heartbeat_to_workers()


    def prune_workers(self):
        """ Look for and kill expired workers.

        Workers are oldest to most recent, so we stop at the first alive
        worker.
        """
        now = time.time()
        while self.idle_workers:
            worker = self.idle_workers[0]
            if worker.expired_at < now:
                self.logger.info('Deleting expired worker: %s', worker.id_)
                self.remove_worker(worker, disconnect=False)
                try:
                    self.idle_workers.remove(worker)
                except ValueError:
                    pass
            else:
                break


    def send_heartbeat_to_workers(self):
        """ Send heartbeats to idle workers. """
        for worker in self.idle_workers:
            self.send_to_worker(worker, MDP.WORKER_V1_HEARTBEAT, None, None)


    async def request_queue_task_worker(self, interval): # msecs
        interval = interval / 1000.0
        processed_requests = []

        while self.running:
            for pos, (service_name, message) in enumerate(self.requests):
                service = self.require_service(service_name)
                if not service.workers:
                    continue

                worker = service.workers.pop(0)
                try:
                    self.idle_workers.remove(worker)
                except ValueError:
                    pass
                self.send_to_worker(worker, MDP.WORKER_V1_REQUEST, None,
                        message)

                processed_requests.append(pos)

            if processed_requests:
                for pos in reversed(processed_requests):
                    self.requests.pop(pos)

                processed_requests = []
            else:
                try:
                    self.request_queue_sleep_task = asyncio.ensure_future(
                            asyncio.sleep(interval, loop=self.loop),
                            loop=self.loop)

                    await self.request_queue_sleep_task
                except asyncio.CancelledError:
                    pass


    def add_idle_worker(self, worker):
        """ This worker is now waiting for work. """
        worker.touch()
        self.idle_workers.append(worker)
        worker.service.add_worker(worker)
        # Force queue worker to run
        self.request_queue_sleep_task.cancel()


    def send_to_worker(self, worker, broker_command, option, message):
        """ Send message to worker.
        """
        if message is None:
            message = []
        elif not is_list_alike(message):
            message = [message]

        # Stack routing and protocol envelopes to start of message
        if option is not None:
            message = [option] + message

        payload = [worker.address, b'', MDP.WORKER_V1, broker_command] +\
                message

        if broker_command != MDP.WORKER_V1_HEARTBEAT:
            self.logger.debug('Sending %s to worker.', broker_command)

        if self.running:
            self.socket.write(payload)
