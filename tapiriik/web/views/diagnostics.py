from django.shortcuts import render, redirect
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD
from tapiriik.database import db
from tapiriik.sync import Sync
from tapiriik.auth import TOTP
from bson.objectid import ObjectId
import hashlib
from datetime import datetime


def diag_requireAuth(view):
    def authWrapper(req, *args, **kwargs):
        if DIAG_AUTH_TOTP_SECRET is not None and DIAG_AUTH_PASSWORD is not None and ("diag_auth" not in req.session or req.session["diag_auth"] is not True):
            return redirect("diagnostics_login")
        return view(req, *args, **kwargs)
    return authWrapper


@diag_requireAuth
def diag_dashboard(req):
    lockedSyncRecords = db.users.aggregate([
                                           {"$match": {"SynchronizationWorker": {"$ne": None}}},
                                           {"$group": {"_id": None, "count": {"$sum": 1}}}
                                           ])
    if len(lockedSyncRecords["result"]) > 0:
        lockedSyncRecords = lockedSyncRecords["result"][0]["count"]
        lockedSyncUsers = list(db.users.find({"SynchronizationWorker": {"$ne": None}}))
    else:
        lockedSyncRecords = 0
        lockedSyncUsers = []

    pendingSynchronizations = db.users.aggregate([
                                                 {"$match": {"NextSynchronization": {"$lt": datetime.utcnow()}}},
                                                 {"$group": {"_id": None, "count": {"$sum": 1}}}
                                                 ])
    if len(pendingSynchronizations["result"]) > 0:
        pendingSynchronizations = pendingSynchronizations["result"][0]["count"]
    else:
        pendingSynchronizations = 0

    userCt = db.users.count()
    autosyncCt = db.users.find({"NextSynchronization": {"$ne": None}}).count()

    errorUsers = list(db.users.find({"SyncErrorCount": {"$gt": 0}}))

    return render(req, "diag/dashboard.html", {"lockedSyncRecords": lockedSyncRecords, "lockedSyncUsers": lockedSyncUsers, "pendingSynchronizations": pendingSynchronizations, "userCt": userCt, "autosyncCt": autosyncCt, "errorUsers": errorUsers})


@diag_requireAuth
def diag_user(req, user):
    userRec = db.users.find_one({"_id": ObjectId(user)})
    delta = False
    if "sync" in req.POST:
        Sync.ScheduleImmediateSync(userRec, req.POST["sync"] == "Full")
        delta = True
    elif "unlock" in req.POST:
        db.users.update({"_id": ObjectId(user)}, {"$unset": {"SynchronizationWorker": None}})
        delta = True
    elif "substitute" in req.POST:
        req.session["substituteUserid"] = user
        return redirect("dashboard")
    elif "svc_unlink" in req.POST:
        from tapiriik.services import Service
        from tapiriik.auth import User
        svcRec = Service.GetServiceRecordByID(req.POST["id"])
        try:
            Service.DeleteServiceRecord(svcRec)
        except:
            pass
        try:
            User.DisconnectService(svcRec)
        except:
            pass
        delta = True
    if delta:
        return redirect("diagnostics_user", user=user)
    return render(req, "diag/user.html", {"user": userRec})


@diag_requireAuth
def diag_unsu(req):
    if "substituteUserid" in req.session:
        user = req.session["substituteUserid"]
        del req.session["substituteUserid"]
        return redirect("diagnostics_user", user=user)
    else:
        return redirect("dashboard")


def diag_login(req):
    if "password" in req.POST:
        if hashlib.sha512(req.POST["password"].encode("utf-8")).hexdigest().upper() == DIAG_AUTH_PASSWORD and TOTP.Get(DIAG_AUTH_TOTP_SECRET) == int(req.POST["totp"]):
            req.session["diag_auth"] = True
            return redirect("diagnostics_dashboard")
    return render(req, "diag/login.html")
