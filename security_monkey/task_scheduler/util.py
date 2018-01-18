"""
.. module: security_monkey.task_scheduler.util
    :platform: Unix
    :synopsis: Instantiates the Celery object for use with task scheduling.

.. version:: $$VERSION$$
.. moduleauthor:: Mike Grima <mgrima@netflix.com>

"""
from __future__ import absolute_import
from celery import Celery
from security_monkey import app


def make_celery(app):
    """
    Recommended from Flask's documentation to set up the Celery object.
    :param app:
    :return:
    """
    celery = Celery(app.import_name)
    celery.config_from_object("celeryconfig")
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery


CELERY = make_celery(app)
