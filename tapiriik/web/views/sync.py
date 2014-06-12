import json
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from tapiriik.auth import User
from tapiriik.sync import Sync
from tapiriik.database import db
from tapiriik.services import Service
from datetime import datetime
import zlib
import random


def sync_status(req):
    if not req.user:
        return HttpResponse(json.dumps({"Hash": random.randint(0,9999)}), mimetype="application/json")

    stats = db.stats.find_one()
    syncHash = 1  # Just used to refresh the dashboard page, until I get on the Angular bandwagon.
    conns = User.GetConnectionRecordsByUser(req.user)
    errorCodes = []
    for conn in conns:
        syncHash = zlib.adler32(bytes(conn.HasExtendedAuthorizationDetails()), syncHash)
        if not hasattr(conn, "SyncErrors"):
            continue
        for err in conn.SyncErrors:
            syncHash = zlib.adler32(bytes(str(err), "UTF-8"), syncHash)
            if "Code" in err and err["Code"] is not None and len(err["Code"]) > 0:
                errorCodes.append(err["Code"])
            else:
                errorCodes.append("SYS-" + err["Step"])

    sync_status_dict = {"NextSync": (req.user["NextSynchronization"].ctime() + " UTC") if "NextSynchronization" in req.user and req.user["NextSynchronization"] is not None else None,
                        "LastSync": (req.user["LastSynchronization"].ctime() + " UTC") if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None else None,
                        "Synchronizing": "SynchronizationWorker" in req.user,
                        "SynchronizationProgress": req.user["SynchronizationProgress"] if "SynchronizationProgress" in req.user else None,
                        "SynchronizationStep": req.user["SynchronizationStep"] if "SynchronizationStep" in req.user else None,
                        "SynchronizationWaitTime": None, # I wish.
                        "Errors": errorCodes,
                        "Hash": syncHash}

    if stats and "QueueHeadTime" in stats:
        sync_status_dict["SynchronizationWaitTime"] = (stats["QueueHeadTime"] - (datetime.utcnow() - req.user["NextSynchronization"]).total_seconds()) if "NextSynchronization" in req.user and req.user["NextSynchronization"] is not None else None

    return HttpResponse(json.dumps(sync_status_dict), mimetype="application/json")

@require_POST
def sync_schedule_immediate(req):
    if not req.user:
        return HttpResponse(status=401)
    if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None and datetime.utcnow() - req.user["LastSynchronization"] < Sync.MinimumSyncInterval:
        return HttpResponse(status=403)
    exhaustive = None
    if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None and datetime.utcnow() - req.user["LastSynchronization"] > Sync.MaximumIntervalBeforeExhaustiveSync:
        exhaustive = True
    Sync.ScheduleImmediateSync(req.user, exhaustive)
    return HttpResponse()

@require_POST
def sync_clear_errorgroup(req, service, group):
    if not req.user:
        return HttpResponse(status=401)

    rec = User.GetConnectionRecord(req.user, service)
    if not rec:
        return HttpResponse(status=404)

    # Prevent this becoming a vehicle for rapid synchronization
    to_clear_count = 0
    for x in rec.SyncErrors:
        if "UserException" in x and "ClearGroup" in x["UserException"] and x["UserException"]["ClearGroup"] == group:
            to_clear_count += 1

    if to_clear_count > 0:
            db.connections.update({"_id": rec._id}, {"$pull":{"SyncErrors":{"UserException.ClearGroup": group}}})
            db.users.update({"_id": req.user["_id"]}, {'$inc':{"BlockingSyncErrorCount":-to_clear_count}}) # In the interests of data integrity, update the summary counts immediately as opposed to waiting for a sync to complete.
            Sync.ScheduleImmediateSync(req.user, True) # And schedule them for an immediate full resynchronization, so the now-unblocked services can be brought up to speed.            return HttpResponse()
            return HttpResponse()

    return HttpResponse(status=404)

@csrf_exempt
@require_POST
def sync_trigger_partial_sync_callback(req, service):
    svc = Service.FromID(service)
    affected_connection_external_ids = svc.ServiceRecordIDsForPartialSyncTrigger(req)
    db.connections.update({"Service": service, "ExternalID": {"$in": affected_connection_external_ids}}, {"$set":{"TriggerPartialSync": True, "TriggerPartialSyncTimestamp": datetime.utcnow()}}, multi=True)
    # Will turn this on once there's a sync-delay option
    # db.users.update({"ConnectedServices.ID": {"$in": affected_connection_ids}}, {"$set": {"NextSynchronization": datetime.utcnow()}}, multi=True) # It would be nicer to use the Sync.Schedule... method, but I want to cleanly do this in bulk
    return HttpResponse(status=204)

