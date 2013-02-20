from django.shortcuts import render, redirect
from django.http import HttpResponse
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD
from tapiriik.database import db
from bson.objectid import ObjectId
import hashlib
import onetimepass
from datetime import datetime

def diag_requireAuth(view):
    def authWrapper(req, *args, **kwargs):
        if "diag_auth" not in req.session or req.session["diag_auth"] != True:
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
    else:
        lockedSyncRecords = 0

    pendingSynchronizations = db.users.aggregate([
                                                {"$match": {"NextSynchronization": {"$lt": datetime.utcnow()}}},
                                                {"$group": {"_id": None, "count": {"$sum": 1}}}
                                                ])
    if len(pendingSynchronizations["result"]) > 0:
        pendingSynchronizations = pendingSynchronizations["result"][0]["count"]
    else:
        pendingSynchronizations = 0

    return render(req, "diag/dashboard.html", {"lockedSyncRecords": lockedSyncRecords, "pendingSynchronizations": pendingSynchronizations})


@diag_requireAuth
def diag_user(req, user):
    userRec = db.users.find_one({"_id": ObjectId(user)})
    return render(req, "diag/user.html", {"user": userRec})


def diag_login(req):
    if "password" in req.POST:
        if hashlib.sha512(req.POST["password"].encode("utf-8")).hexdigest().upper() == DIAG_AUTH_PASSWORD and onetimepass.valid_totp(req.POST["totp"], DIAG_AUTH_TOTP_SECRET):
            req.session["diag_auth"] = True
            return redirect("diagnostics_dashboard")
    return render(req, "diag/login.html")
