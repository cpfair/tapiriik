import json
from django.http import HttpResponse
from tapiriik.auth import User
from tapiriik.sync import Sync
from datetime import datetime
import zlib


def sync_status(req):
    if not req.user:
        return HttpResponse(status=403)

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
    return HttpResponse(json.dumps({"NextSync": (req.user["NextSynchronization"].ctime() + " UTC") if "NextSynchronization" in req.user and req.user["NextSynchronization"] is not None else None,
                                    "LastSync": (req.user["LastSynchronization"].ctime() + " UTC") if "LastSynchronization" in req.user and req.user["LastSynchronization"] is not None else None,
                                    "Synchronizing": "SynchronizationWorker" in req.user,
                                    "SynchronizationProgress": req.user["SynchronizationProgress"] if "SynchronizationProgress" in req.user else None,
                                    "Errors": errorCodes,
                                    "Hash": syncHash}), mimetype="application/json")


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
