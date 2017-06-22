from tapiriik.database import db, close_connections
from tapiriik.settings import RABBITMQ_BROKER_URL, MONGO_FULL_WRITE_CONCERN
from datetime import datetime
from celery import Celery
from celery.signals import worker_shutdown

class _celeryConfig:
    CELERY_ROUTES = {
        "sync_remote_triggers.trigger_remote": {"queue": "tapiriik-remote-trigger"}
    }
    CELERYD_CONCURRENCY = 1 # Otherwise the GC rate limiting breaks since file locking is per-process.
    CELERYD_PREFETCH_MULTIPLIER = 1 # The message queue could use some exercise.

celery_app = Celery('sync_remote_triggers', broker=RABBITMQ_BROKER_URL)
celery_app.config_from_object(_celeryConfig())

@worker_shutdown.connect
def celery_shutdown(**kwargs):
    close_connections()

@celery_app.task(acks_late=True)
def trigger_remote(service_id, affected_connection_external_ids):
    from tapiriik.auth import User
    from tapiriik.services import Service
    svc = Service.FromID(service_id)
    db.connections.update({"Service": svc.ID, "ExternalID": {"$in": affected_connection_external_ids}}, {"$set":{"TriggerPartialSync": True, "TriggerPartialSyncTimestamp": datetime.utcnow()}}, multi=True, w=MONGO_FULL_WRITE_CONCERN)
    affected_connection_ids = db.connections.find({"Service": svc.ID, "ExternalID": {"$in": affected_connection_external_ids}}, {"_id": 1})
    affected_connection_ids = [x["_id"] for x in affected_connection_ids]
    trigger_users_query = User.PaidUserMongoQuery()
    trigger_users_query.update({"ConnectedServices.ID": {"$in": affected_connection_ids}})
    trigger_users_query.update({"Config.suppress_auto_sync": {"$ne": True}})
    db.users.update(trigger_users_query, {"$set": {"NextSynchronization": datetime.utcnow()}}, multi=True) # It would be nicer to use the Sync.Schedule... method, but I want to cleanly do this in bulk
