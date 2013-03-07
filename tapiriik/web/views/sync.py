import json
from django.http import HttpResponse
from tapiriik.auth import User
from tapiriik.sync import Sync
from datetime import datetime

def sync_status(req):
    if not req.user:
        return HttpResponse(status=401)

    conns = User.GetConnectionRecordsByUser(req.user)
    totalErrors = 0
    for conn in conns:
        if "SyncErrors" not in conn:
            continue
        totalErrors += len(conn["SyncErrors"])
    print(req.user)
    return HttpResponse(json.dumps({"NextSync": (req.user["NextSynchronization"].ctime() + " UTC") if "NextSynchronization" in req.user else None,
                                    "LastSync": (req.user["LastSynchronization"].ctime() + " UTC") if "LastSynchronization" in req.user else None,
                                    "Synchronizing": "SynchronizationWorker" in req.user,
                                    "Errors": totalErrors}), mimetype="application/json")

def sync_schedule_immediate(req):
    if not req.user:
        return HttpResponse(status=401)
    if (datetime.utcnow() - req.user["LastSynchronization"] < Sync.MinimumSyncInterval):
        return HttpResponse(status=403)
    Sync.ScheduleImmediateSync(req.user)
    return HttpResponse(json.dumps({"success": True}))
