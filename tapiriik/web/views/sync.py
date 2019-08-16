import json
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import redirect
from tapiriik.auth import User
from tapiriik.sync import Sync, SynchronizationTask
from tapiriik.database import db
from tapiriik.services import Service
from tapiriik.settings import MONGO_FULL_WRITE_CONCERN
from datetime import datetime
import zlib


def sync_status(req):
    if not req.user:
        return HttpResponse(status=403)

    stats = db.stats.find_one()
    syncHash = 1  # Just used to refresh the dashboard page, until I get on the Angular bandwagon.
    conns = User.GetConnectionRecordsByUser(req.user)

    def svc_id(svc):
        return svc.Service.ID

    def err_msg(err):
        return err["Message"]

    for conn in sorted(conns, key=svc_id):
        syncHash = zlib.adler32(bytes(conn.HasExtendedAuthorizationDetails()), syncHash)
        if not hasattr(conn, "SyncErrors"):
            continue
        for err in sorted(conn.SyncErrors, key=err_msg):
            syncHash = zlib.adler32(bytes(err_msg(err), "UTF-8"), syncHash)

    # Flatten NextSynchronization with QueuedAt
    pendingSyncTime = req.user["NextSynchronization"] if "NextSynchronization" in req.user else None
    if "QueuedAt" in req.user and req.user["QueuedAt"]:
        pendingSyncTime = req.user["QueuedAt"]

    sync_status_dict = {"NextSync": (pendingSyncTime.ctime() + " UTC") if pendingSyncTime else None,
                        "LastSync": (req.user["LastSynchronization"].ctime() + " UTC") if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None else None,
                        "Synchronizing": "SynchronizationWorker" in req.user,
                        "SynchronizationProgress": req.user["SynchronizationProgress"] if "SynchronizationProgress" in req.user else None,
                        "SynchronizationStep": req.user["SynchronizationStep"] if "SynchronizationStep" in req.user else None,
                        "SynchronizationWaitTime": None, # I wish.
                        "Hash": syncHash}

    if stats and "QueueHeadTime" in stats:
        sync_status_dict["SynchronizationWaitTime"] = (stats["QueueHeadTime"] - (datetime.utcnow() - req.user["NextSynchronization"]).total_seconds()) if "NextSynchronization" in req.user and req.user["NextSynchronization"] is not None else None

    return HttpResponse(json.dumps(sync_status_dict), content_type="application/json")

def sync_recent_activity(req):
    if not req.user:
        return HttpResponse(status=403)
    res = SynchronizationTask.RecentSyncActivity(req.user)
    return HttpResponse(json.dumps(res), content_type="application/json")

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

@require_POST
def sync_clear_badactivitiesacknowledgement(req):
    if not req.user:
        return HttpResponse(status=401)

    db.users.update({"_id": req.user["_id"]}, {'$unset':{"BlockedOnBadActivitiesAcknowledgement": None}})
    return redirect("dashboard")

@csrf_exempt
def sync_trigger_partial_sync_callback(req, service):
    svc = Service.FromID(service)
    if req.method == "POST":
        from sync_remote_triggers import trigger_remote
        affected_connection_external_ids = svc.ExternalIDsForPartialSyncTrigger(req)
        trigger_remote.apply_async(args=[service, affected_connection_external_ids])
        return HttpResponse(status=svc.PartialSyncTriggerStatusCode)
    elif req.method == "GET":
        return svc.PartialSyncTriggerGET(req)
    else:
        return HttpResponse(status=400)
