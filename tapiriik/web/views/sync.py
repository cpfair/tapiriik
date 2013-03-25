import json
from django.http import HttpResponse
from tapiriik.auth import User
from tapiriik.sync import Sync
from datetime import datetime


def sync_status(req):
    if not req.user:
        return HttpResponse(status=403)

    conns = User.GetConnectionRecordsByUser(req.user)
    errorCodes = []
    for conn in conns:
        if "SyncErrors" not in conn:
            continue
        for err in conn["SyncErrors"]:
            if "Code" in err and err["Code"] is not None and len(err["Code"]) > 0:
                errorCodes.append(err["Code"])
            else:
                errorCodes.append("SYS-" + err["Step"])
    return HttpResponse(json.dumps({"NextSync": (req.user["NextSynchronization"].ctime() + " UTC") if "NextSynchronization" in req.user and req.user["NextSynchronization"] is not None else None,
                                    "LastSync": (req.user["LastSynchronization"].ctime() + " UTC") if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None else None,
                                    "Synchronizing": "SynchronizationWorker" in req.user,
                                    "Errors": errorCodes}), mimetype="application/json")


def sync_schedule_immediate(req):
    if not req.user:
        return HttpResponse(status=401)
    if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None and datetime.utcnow() - req.user["LastSynchronization"] < Sync.MinimumSyncInterval:
        return HttpResponse(status=403)
    Sync.ScheduleImmediateSync(req.user)
    return HttpResponse()
