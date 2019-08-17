from tapiriik.database import db, close_connections
from tapiriik.requests_lib import patch_requests_source_address
from tapiriik.settings import RABBITMQ_BROKER_URL, MONGO_HOST, MONGO_FULL_WRITE_CONCERN
from tapiriik import settings
from datetime import datetime

if isinstance(settings.HTTP_SOURCE_ADDR, list):
    settings.HTTP_SOURCE_ADDR = settings.HTTP_SOURCE_ADDR[0]
    patch_requests_source_address((settings.HTTP_SOURCE_ADDR, 0))

from tapiriik.services import Service
from celery import Celery
from celery.signals import worker_shutdown
from datetime import datetime

class _celeryConfig:
    CELERY_ROUTES = {
        "sync_poll_triggers.trigger_poll": {"queue": "tapiriik-poll"}
    }
    CELERYD_CONCURRENCY = 1 # Otherwise the GC rate limiting breaks since file locking is per-process.
    CELERYD_PREFETCH_MULTIPLIER = 1 # The message queue could use some exercise.

celery_app = Celery('sync_poll_triggers', broker=RABBITMQ_BROKER_URL)
celery_app.config_from_object(_celeryConfig())

@worker_shutdown.connect
def celery_shutdown(**kwargs):
    close_connections()

@celery_app.task(acks_late=True)
def trigger_poll(service_id, index):
    from tapiriik.auth import User
    print("Polling %s-%d" % (service_id, index))
    svc = Service.FromID(service_id)
    affected_connection_external_ids = svc.PollPartialSyncTrigger(index)
    print("Triggering %d connections via %s-%d" % (len(affected_connection_external_ids), service_id, index))

    # MONGO_FULL_WRITE_CONCERN because there was a race where users would get picked for synchronization before their service record was updated on the correct secondary
    # So it'd think the service wasn't triggered
    db.connections.update({"Service": service_id, "ExternalID": {"$in": affected_connection_external_ids}}, {"$set":{"TriggerPartialSync": True, "TriggerPartialSyncTimestamp": datetime.utcnow()}}, multi=True, w=MONGO_FULL_WRITE_CONCERN)

    affected_connection_ids = db.connections.find({"Service": svc.ID, "ExternalID": {"$in": affected_connection_external_ids}}, {"_id": 1})
    affected_connection_ids = [x["_id"] for x in affected_connection_ids]
    trigger_users_query = User.PaidUserMongoQuery()
    trigger_users_query.update({"ConnectedServices.ID": {"$in": affected_connection_ids}})
    trigger_users_query.update({"Config.suppress_auto_sync": {"$ne": True}})
    db.users.update(trigger_users_query, {"$set": {"NextSynchronization": datetime.utcnow()}}, multi=True) # It would be nicer to use the Sync.Schedule... method, but I want to cleanly do this in bulk

    db.poll_stats.insert({"Service": service_id, "Index": index, "Timestamp": datetime.utcnow(), "TriggerCount": len(affected_connection_external_ids)})

def schedule_trigger_poll():
    schedule_data = list(db.trigger_poll_scheduling.find())
    print("Scheduler run at %s" % datetime.now())
    for svc in Service.List():
        if svc.PartialSyncTriggerRequiresPolling and svc.ID not in DISABLED_SERVICES:
            print("Checking %s's %d poll indexes" % (svc.ID, svc.PartialSyncTriggerPollMultiple))
            for idx in range(svc.PartialSyncTriggerPollMultiple):
                svc_schedule = [x for x in schedule_data if x["Service"] == svc.ID and x["Index"] == idx]
                if not svc_schedule:
                    svc_schedule = {"Service": svc.ID, "Index": idx, "LastScheduled": datetime.min}
                else:
                    svc_schedule = svc_schedule[0]

                if datetime.utcnow() - svc_schedule["LastScheduled"] > svc.PartialSyncTriggerPollInterval:
                    print("Scheduling %s-%d" % (svc.ID, idx))
                    svc_schedule["LastScheduled"] = datetime.utcnow()
                    trigger_poll.apply_async(args=[svc.ID, idx], expires=svc.PartialSyncTriggerPollInterval.total_seconds(), time_limit=svc.PartialSyncTriggerPollInterval.total_seconds())
                    db.trigger_poll_scheduling.update({"Service": svc.ID, "Index": idx}, svc_schedule, upsert=True)

if __name__ == "__main__":
    schedule_trigger_poll()
    close_connections()