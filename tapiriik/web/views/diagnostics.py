from django.shortcuts import render, redirect
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD
from tapiriik.database import db
from tapiriik.sync import Sync
from tapiriik.auth import TOTP
from bson.objectid import ObjectId
import hashlib
import json
from datetime import datetime, timedelta


def diag_requireAuth(view):
    def authWrapper(req, *args, **kwargs):
        if DIAG_AUTH_TOTP_SECRET is not None and DIAG_AUTH_PASSWORD is not None and ("diag_auth" not in req.session or req.session["diag_auth"] is not True):
            return redirect("diagnostics_login")
        return view(req, *args, **kwargs)
    return authWrapper


@diag_requireAuth
def diag_dashboard(req):


    context = {}
    lockedSyncRecords = db.users.aggregate([
                                           {"$match": {"SynchronizationWorker": {"$ne": None}}},
                                           {"$group": {"_id": None, "count": {"$sum": 1}}}
                                           ])
    if len(lockedSyncRecords["result"]) > 0:
        context["lockedSyncRecords"] = lockedSyncRecords["result"][0]["count"]
        context["lockedSyncUsers"] = list(db.users.find({"SynchronizationWorker": {"$ne": None}}))
    else:
        context["lockedSyncRecords"] = 0
        context["lockedSyncUsers"] = []

    pendingSynchronizations = db.users.aggregate([
                                                 {"$match": {"NextSynchronization": {"$lt": datetime.utcnow()}}},
                                                 {"$group": {"_id": None, "count": {"$sum": 1}}}
                                                 ])
    if len(pendingSynchronizations["result"]) > 0:
        context["pendingSynchronizations"] = pendingSynchronizations["result"][0]["count"]
    else:
        context["pendingSynchronizations"] = 0

    context["userCt"] = db.users.count()
    context["autosyncCt"] = db.users.find({"NextSynchronization": {"$ne": None}}).count()

    context["errorUsers"] = list(db.users.find({"NonblockingSyncErrorCount": {"$gt": 0}}))
    context["exclusionUsers"] = list(db.users.find({"SyncExclusionCount": {"$gt": 0}}))

    context["allWorkers"] = list(db.sync_workers.find())
    context["allWorkerPIDs"] = [x["Process"] for x in context["allWorkers"]]
    context["activeWorkers"] = [x for x in context["allWorkers"] if x["Heartbeat"] > datetime.utcnow() - timedelta(seconds=30)]
    context["stalledWorkers"] = [x for x in context["allWorkers"] if x["Heartbeat"] < datetime.utcnow() - timedelta(seconds=30)]
    context["stalledWorkerPIDs"] = [x["Process"] for x in context["stalledWorkers"]]

    syncErrorListing = list(db.common_sync_errors.find().sort("value", -1))
    syncErrorsAffectingServices = [service for error in syncErrorListing for service in error["value"]["connections"]]
    syncErrorsAffectingUsers = list(db.users.find({"ConnectedServices.ID": {"$in": syncErrorsAffectingServices}}))
    syncErrorSummary = []
    autoSyncErrorSummary = []
    for error in syncErrorListing:
        serviceSet = set(error["value"]["connections"])
        affected_auto_users = [{"id":user["_id"], "highlight": user["LastSynchronization"] > datetime.utcnow() - timedelta(minutes=5)} for user in syncErrorsAffectingUsers if set([conn["ID"] for conn in user["ConnectedServices"]]) & serviceSet and "NextSynchronization" in user and user["NextSynchronization"] is not None]
        affected_users = [{"id": user["_id"], "highlight": False} for user in syncErrorsAffectingUsers if set([conn["ID"] for conn in user["ConnectedServices"]]) & serviceSet and ("NextSynchronization" not in user or user["NextSynchronization"] is None)]
        if len(affected_auto_users):
            autoSyncErrorSummary.append({"message": error["value"]["exemplar"], "count": int(error["value"]["count"]), "affected_users": affected_auto_users})
        if len(affected_users):
            syncErrorSummary.append({"message": error["value"]["exemplar"], "count": int(error["value"]["count"]), "affected_users": affected_users})

    context["autoSyncErrorSummary"] = autoSyncErrorSummary
    context["syncErrorSummary"] = syncErrorSummary

    delta = False
    if "deleteStalledWorker" in req.POST:
        db.sync_workers.remove({"Process": int(req.POST["pid"])})
        delta = True
    if "unlockOrphaned" in req.POST:
        orphanedUserIDs = [x["_id"] for x in context["lockedSyncUsers"] if x["SynchronizationWorker"] not in context["allWorkerPIDs"]]
        db.users.update({"_id":{"$in":orphanedUserIDs}}, {"$unset": {"SynchronizationWorker": None}}, multi=True)
        delta = True

    if delta:
        return redirect("diagnostics_dashboard")

    return render(req, "diag/dashboard.html", context)


@diag_requireAuth
def diag_user(req, user):
    try:
        userRec = db.users.find_one({"_id": ObjectId(user)})
    except:
        userRec = None
    if not userRec:
        searchOpts = [{"Payments.Txn": user}, {"Payments.Email": user}]
        try:
            searchOpts.append({"AncestorAccounts": ObjectId(user)})
            searchOpts.append({"ConnectedServices.ID": ObjectId(user)})
        except:
            pass # Invalid format for ObjectId
        userRec = db.users.find_one({"$or": searchOpts})
        if not userRec:
            searchOpts = [{"ExternalID": user}]
            try:
                searchOpts.append({"ExternalID": int(user)})
            except:
                pass # Not an int
            svcRec = db.connections.find_one({"$or": searchOpts})
            if svcRec:
                userRec = db.users.find_one({"ConnectedServices.ID": svcRec["_id"]})
        if userRec:
            return redirect("diagnostics_user", user=userRec["_id"])
    if not userRec:
        return render(req, "diag/error_user_not_found.html")
    delta = True # Easier to set this to false in the one no-change case.
    if "sync" in req.POST:
        Sync.ScheduleImmediateSync(userRec, req.POST["sync"] == "Full")
    elif "unlock" in req.POST:
        db.users.update({"_id": ObjectId(user)}, {"$unset": {"SynchronizationWorker": None}})
    elif "lock" in req.POST:
        db.users.update({"_id": ObjectId(user)}, {"$set": {"SynchronizationWorker": 1}})
    elif "hostrestrict" in req.POST:
        host = req.POST["host"]
        if host:
            db.users.update({"_id": ObjectId(user)}, {"$set": {"SynchronizationHostRestriction": host}})
        else:
            db.users.update({"_id": ObjectId(user)}, {"$unset": {"SynchronizationHostRestriction": None}})
    elif "substitute" in req.POST:
        req.session["substituteUserid"] = user
        return redirect("dashboard")
    elif "svc_setauth" in req.POST and len(req.POST["authdetails"]):
        db.connections.update({"_id": ObjectId(req.POST["id"])}, {"$set":{"Authorization": json.loads(req.POST["authdetails"])}})
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
    elif "svc_marksync" in req.POST:
        db.connections.update({"_id": ObjectId(req.POST["id"])},
                              {"$addToSet": {"SynchronizedActivities": req.POST["uid"]}},
                              multi=False)
    elif "svc_clearexc" in req.POST:
        db.connections.update({"_id": ObjectId(req.POST["id"])}, {"$unset": {"ExcludedActivities": 1}})
    elif "svc_clearacts" in req.POST:
        db.connections.update({"_id": ObjectId(req.POST["id"])}, {"$unset": {"SynchronizedActivities": 1}})
        Sync.SetNextSyncIsExhaustive(userRec, True)
    else:
        delta = False

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

@diag_requireAuth
def diag_payments(req):
    payments = list(db.payments.find())
    for payment in payments:
        payment["Accounts"] = [x["_id"] for x in db.users.find({"Payments.Txn": payment["Txn"]}, {"_id":1})]
    return render(req, "diag/payments.html", {"payments": payments})

def diag_login(req):
    if "password" in req.POST:
        if hashlib.sha512(req.POST["password"].encode("utf-8")).hexdigest().upper() == DIAG_AUTH_PASSWORD and TOTP.Get(DIAG_AUTH_TOTP_SECRET) == int(req.POST["totp"]):
            req.session["diag_auth"] = True
            return redirect("diagnostics_dashboard")
    return render(req, "diag/login.html")
