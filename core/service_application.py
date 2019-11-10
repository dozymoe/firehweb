import asyncio
from collections import MutableMapping
import logging
from signal import SIGINT, SIGTERM

from ..misc import configuration


class Application(MutableMapping):

    def __init__(self, loop=None):
        self._storage = dict()
        self.loop = loop or asyncio.get_event_loop()
        self.on_startup = []
        self.on_shutdown = []
        self.on_cleanup = []

        self._run_event = asyncio.Event()
        self.logger = logging.getLogger('app')

        loop.add_signal_handler(SIGINT, self._signal_handler)
        loop.add_signal_handler(SIGTERM, self._signal_handler)

    def __getitem__(self, key):
        return self._storage[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self._storage[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self._storage[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._storage)

    def __len__(self):
        return len(self._storage)

    def __keytransform__(self, key):
        return key

    def _signal_handler(self):
        self._run_event.set()

    def run(self):
        self.logger.info('Starting...')

        for callback in self.on_startup:
            self.loop.run_until_complete(callback(self))

        self.loop.run_until_complete(self._run_event.wait())

        for callback in self.on_shutdown:
            self.loop.run_until_complete(callback(self))
        for callback in self.on_cleanup:
            self.loop.run_until_complete(callback(self))

        self.logger.info('Quit.')

    def stop(self):
        self._run_event.set()


async def startup(app):
    config = app['config']

    if 'redis.cache.connect' in config:
        import aioredis #pylint:disable=import-outside-toplevel
        app['redis_cache'] = await aioredis.create_pool((
                config.get('redis.cache.connect', '127.0.0.1'),
                config.get('redis.cache.port', 8004)))

    if 'redis.persist.connect' in config:
        import aioredis #pylint:disable=import-outside-toplevel
        app['redis_persist'] = await aioredis.create_pool((
                config.get('redis.persist.connect', '127.0.0.1'),
                config.get('redis.persist.port', 8005)))

    if 'database.name' in config:
        import aiopg #pylint:disable=import-outside-toplevel
        app['db'] = await aiopg.create_pool(dsn=' '.join('='.join(
                str(y) for y in x) for x in dict(
                dbname=config['database.name'],
                host=config.get('database.host', 'localhost'),
                port=config.get('database.port', 5432),
                user=config.get('database.username'),
                password=config.get('database.password')).items()),
                loop=app.loop)


async def shutdown(app):
    if 'db' in app:
        app['db'].close()

    if 'redis_cache' in app:
        app['redis_cache'].close()

    if 'redis_persist' in app:
        app['redis_persist'].close()


async def cleanup(app):
    if 'db' in app:
        app['db'].close()
        await app['db'].wait_closed()

    if 'redis_cache' in app:
        await app['redis_cache'].wait_closed()

    if 'redis_persist' in app:
        await app['redis_persist'].wait_closed()


def create_application(base_dir, app_class=Application):
    # Setup config
    config = {}
    configuration.load_files_from_shell(config, configuration.generic_adapter)

    # AsyncIO loop
    loop = asyncio.get_event_loop()

    # Logging
    loglevel = getattr(logging, config.get('logging.level', 'DEBUG'))
    logging.basicConfig(level=loglevel)

    app = app_class(loop=loop)
    app['config'] = config
    app['base_dir'] = base_dir

    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    app.on_cleanup.append(cleanup)
    return app


def run(app, run_callback=None):
    if run_callback:
        run_callback(app)
    else:
        app.run()
