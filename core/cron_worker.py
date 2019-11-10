import logging

_logger = logging.getLogger(__name__)


async def run_jobs(app):
    for job_class in app['jobs']:
        try:
            job = job_class(app)
            await job.validate()
            if not job.errors:
                _logger.info("Running %s...", job_class.__name__)
                await job.perform()
        except: # pylint:disable=bare-except
            _logger.exception("Error running cronjob %s.", job_class.__name__)

    app.stop()
