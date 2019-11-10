import os
import jinja2

from .service_application import create_application as base_create
from .service_application import run #pylint:disable=unused-import


def create_application(base_dir):
    app = base_create(base_dir)
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

    jinja2_env = jinja2.Environment(**env_args)
    # globals
    jinja2_env.globals.update({})
    # filters
    jinja2_env.filters.update({})

    app['jinja2'] = jinja2_env

    return app
