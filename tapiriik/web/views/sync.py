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
        totalErrors += len(conn["SyncErrors"])
    return HttpResponse(json.dumps({"NextSync":req.user["NextSynchronization"].ctime()+" UTC",
                                    "LastSync":req.user["LastSynchronization"].ctime()+" UTC",
                                    "Errors":totalErrors}))

def sync_schedule_immediate(req):
    if not req.user:
        return HttpResponse(status=401)
    if (datetime.utcnow() - req.user["LastSynchronization"] < Sync.MinimumSyncInterval):
        return HttpResponse(status=403)
    Sync.ScheduleImmediateSync(req.user)
    return HttpResponse(json.dumps({"success": True}))
