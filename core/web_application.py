import os
import aiohttp_jinja2
import aiohttp.web
import jinja2

from .service_application import create_application as base_create
from .service_application import run as base_run


def create_application(base_dir):
    app = base_create(base_dir, app_class=aiohttp.web.Application)
    config = app['config']

    ## Templates

    env_args = {}

    template_dirs = [os.path.join(os.environ['ROOT_DIR'], f) for f in\
            config.get('jinja2.template_dirs', [])]
    env_args['loader'] = jinja2.FileSystemLoader(template_dirs)

    cache_dir = config.get('jinja2.cache_dir')
    if cache_dir:
        cache_dir = os.path.join(os.environ['ROOT_DIR'], cache_dir)
        env_args['bytecode_cache'] = jinja2.FileSystemBytecodeCache(cache_dir)

    aiohttp_jinja2.setup(app, **env_args)

    return app


def run_web(app):
    config = app['config']
    aiohttp.web.run_app(
            app,
            host=config.get('server.listen', 'localhost'),
            port=config.get('server.port', 8000),
            access_log=None)


def run(app):
    base_run(app, run_web)
