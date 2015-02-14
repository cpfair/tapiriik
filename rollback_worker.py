from tapiriik.database import db, close_connections
from tapiriik.settings import RABBITMQ_BROKER_URL, MONGO_HOST, MONGO_FULL_WRITE_CONCERN
from tapiriik import settings
from datetime import datetime

from celery import Celery
from celery.signals import worker_shutdown
from datetime import datetime

class _celeryConfig:
    CELERY_ROUTES = {
        "rollback_worker.rollback_task": {"queue": "tapiriik-rollback"}
    }
    CELERYD_CONCURRENCY = 1
    CELERYD_PREFETCH_MULTIPLIER = 1

celery_app = Celery('rollback_worker', broker=RABBITMQ_BROKER_URL)
celery_app.config_from_object(_celeryConfig())

@worker_shutdown.connect
def celery_shutdown():
    close_connections()

@celery_app.task()
def rollback_task(task_id):
    from tapiriik.services.rollback import RollbackTask
    print("Starting rollback task %s" % task_id)
    task = RollbackTask.Get(task_id)
    task.Execute()

def schedule_rollback_task(task_id):
    rollback_task.apply_async(args=[task_id])
