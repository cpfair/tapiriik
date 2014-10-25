from django.shortcuts import render, redirect
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD, SITE_VER
from tapiriik.database import db
from tapiriik.sync import Sync
from tapiriik.auth import TOTP, DiagnosticsUser, User
from bson.objectid import ObjectId
import hashlib
import json
from datetime import datetime, timedelta

def diag_requireAuth(view):
    def authWrapper(req, *args, **kwargs):
        if not DiagnosticsUser.IsAuthenticated(req):
            return redirect("diagnostics_login")
        return view(req, *args, **kwargs)
    return authWrapper

@diag_requireAuth
def diag_dashboard(req):
    context = {}
    stats = db.stats.find_one()

    stall_timeout = timedelta(minutes=1)

    # We fetch this twice so the (orphaned) indicators are correct even if there were writes during all these other queries
    context["allWorkerPIDsPre"] = [x["Process"] for x in db.sync_workers.find()]

    context["lockedSyncUsers"] = list(db.users.find({"SynchronizationWorker": {"$ne": None}}))
    context["lockedSyncRecords"] = len(context["lockedSyncUsers"])
    context["queuedUnlockedUsers"] = list(db.users.find({"SynchronizationWorker": {"$exists": False}, "QueuedAt": {"$ne": None}}))

    context["pendingSynchronizations"] = db.users.find({"NextSynchronization": {"$lt": datetime.utcnow()}}).count()
    context["pendingSynchronizationsLocked"] = db.users.find({"NextSynchronization": {"$lt": datetime.utcnow()}, "SynchronizationWorker": {"$ne": None}}).count()
    context["pendingSynchronizationsLockedQueued"] = db.users.find({"NextSynchronization": {"$lt": datetime.utcnow()}, "QueuedAt": {"$ne": None, "$exists": True}, "SynchronizationWorker": {"$ne": None}}).count()
    context["pendingSynchronizationsQueued"] = db.users.find({"NextSynchronization": {"$lt": datetime.utcnow()}, "QueuedAt": {"$ne": None, "$exists": True}}).count()
    context["queuedSynchronizations"] = db.users.find({"QueuedAt": {"$lt": datetime.utcnow()}}).count()
    context["queuedSynchronizationsLocked"] = db.users.find({"QueuedAt": {"$lt": datetime.utcnow()}, "SynchronizationWorker": {"$ne": None}}).count()

    context["userCt"] = db.users.count()
    context["scheduledCt"] = db.users.find({"$or":[{"NextSynchronization": {"$ne": None, "$exists": True}}, {"QueuedAt": {"$ne": None, "$exists": True}}]}).count()
    context["autosyncCt"] = db.users.find(User.PaidUserMongoQuery()).count()

    context["errorUsersCt"] = db.users.find({"NonblockingSyncErrorCount": {"$gt": 0}}).count()
    context["exclusionUsers"] = db.users.find({"SyncExclusionCount": {"$gt": 0}}).count()

    context["allWorkers"] = list(db.sync_workers.find())

    synchronizingUserIds = [x["User"] if "User" in x else None for x in context["allWorkers"]]
    context["duplicatedUserSynchronizations"] = set([x for x in synchronizingUserIds if synchronizingUserIds.count(x) > 1])

    context["hostWorkerCount"] = {host:len([1 for x in context["allWorkers"] if x["Host"] == host]) for host in set([x["Host"] for x in context["allWorkers"]])}

    # Each worker can be engaged for <= 60*60 seconds in an hour
    if len(context["allWorkers"]) > 0 and stats:
        context["loadFactor"] = stats["TotalSyncTimeUsed"] / (len(context["allWorkers"]) * 60 * 60)
    else:
        context["loadFactor"] = 0

    context["allWorkerPIDs"] = [x["Process"] for x in context["allWorkers"]]
    context["activeWorkers"] = [x for x in context["allWorkers"] if x["Heartbeat"] > datetime.utcnow() - stall_timeout]

    context["workerStates"]= {}
    workerStates = set(x["State"] for x in context["allWorkers"])
    for state in workerStates:
        context["workerStates"][state] = len([x for x in context["allWorkers"] if x["State"] == state])


    context["stalledWorkers"] = [x for x in context["allWorkers"] if x["Heartbeat"] < datetime.utcnow() - stall_timeout]
    context["stalledWorkerPIDs"] = [x["Process"] for x in context["stalledWorkers"]]

    delta = False
    if "deleteStalledWorker" in req.POST:
        db.sync_workers.remove({"Process": int(req.POST["pid"])})
        delta = True
    if "unlockOrphaned" in req.POST:
        orphanedUserIDs = [x["_id"] for x in context["lockedSyncUsers"] if x["SynchronizationWorker"] not in context["allWorkerPIDs"]]
        db.users.update({"_id":{"$in":orphanedUserIDs}}, {"$unset": {"SynchronizationWorker": None}}, multi=True)
        delta = True
    if "requeueQueued" in req.POST:
        db.users.update({"QueuedAt": {"$lt": datetime.utcnow()}, "$or": [{"SynchronizationWorker": {"$exists": False}}, {"SynchronizationWorker": None}]}, {"$set": {"NextSynchronization": datetime.utcnow(), "QueuedGeneration": "manual"}, "$unset": {"QueuedAt": True}}, multi=True)

    if delta:
        return redirect("diagnostics_dashboard")

    return render(req, "diag/dashboard.html", context)

@diag_requireAuth
def diag_errors(req):
    context = {}
    syncErrorListing = list(db.common_sync_errors.find().sort("value.count", -1))
    syncErrorsAffectingServices = [service for error in syncErrorListing for service in error["value"]["connections"]]
    syncErrorsAffectingUsers = list(db.users.find({"ConnectedServices.ID": {"$in": syncErrorsAffectingServices}}))
    syncErrorSummary = []
    autoSyncErrorSummary = []
    for error in syncErrorListing:
        serviceSet = set(error["value"]["connections"])
        affected_auto_users = [{"id": user["_id"], "highlight": "LastSynchronization" in user and user["LastSynchronization"] > datetime.utcnow() - timedelta(minutes=5), "outdated": user["LastSynchronizationVersion"] != SITE_VER if "LastSynchronizationVersion" in user else True} for user in syncErrorsAffectingUsers if set([conn["ID"] for conn in user["ConnectedServices"]]) & serviceSet and "NextSynchronization" in user and user["NextSynchronization"] is not None]
        affected_users = [{"id": user["_id"], "highlight": False, "outdated": False} for user in syncErrorsAffectingUsers if set([conn["ID"] for conn in user["ConnectedServices"]]) & serviceSet and ("NextSynchronization" not in user or user["NextSynchronization"] is None)]
        if len(affected_auto_users):
            autoSyncErrorSummary.append({"service": error["_id"]["service"], "message": error["value"]["exemplar"], "count": int(error["value"]["count"]), "affected_users": affected_auto_users})
        if len(affected_users):
            syncErrorSummary.append({"service": error["_id"]["service"], "message": error["value"]["exemplar"], "count": int(error["value"]["count"]), "affected_users": affected_users})

    context["autoSyncErrorSummary"] = autoSyncErrorSummary
    context["syncErrorSummary"] = syncErrorSummary

    return render(req, "diag/errors.html", context)


@diag_requireAuth
def diag_graphs(req):
    context = {}
    stats_series = list(db.sync_status_stats.find().sort("$natural", -1).limit(24 * 6)) # Last 24 hours (assuming 10 min intervals, monotonic timestamps)
    for item in stats_series:
        item["Timestamp"] = item["Timestamp"].strftime("%H:%M")
        del item["_id"]
    context["dataSeriesJSON"] = json.dumps(stats_series)
    return render(req, "diag/graphs.html", context)

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
    elif "requeue" in req.POST:
        db.users.update({"_id": ObjectId(user)}, {"$unset": {"QueuedAt": None}})
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
    elif "svc_setconfig" in req.POST and len(req.POST["config"]):
        db.connections.update({"_id": ObjectId(req.POST["id"])}, {"$set":{"Config": json.loads(req.POST["config"])}})
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
    elif "svc_toggle_poll_sub" in req.POST:
        from tapiriik.services import Service
        svcRec = Service.GetServiceRecordByID(req.POST["id"])
        svcRec.SetPartialSyncTriggerSubscriptionState(not svcRec.PartialSyncTriggerSubscribed)
    elif "svc_tryagain" in req.POST:
        from tapiriik.services import Service
        svcRec = Service.GetServiceRecordByID(req.POST["id"])
        db.connections.update({"_id": ObjectId(req.POST["id"])}, {"$pull": {"SyncErrors": {"Scope": "activity"}}})
        act_recs = db.activity_records.find_one({"UserID": ObjectId(user)})
        for act in act_recs["Activities"]:
            if "FailureCounts" in act and svcRec.Service.ID in act["FailureCounts"]:
                del act["FailureCounts"][svcRec.Service.ID]
        db.activity_records.save(act_recs)
    else:
        delta = False

    if delta:
        return redirect("diagnostics_user", user=user)
    return render(req, "diag/user.html", {"diag_user": userRec})


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
            DiagnosticsUser.Authorize(req)
            return redirect("diagnostics_dashboard")
    return render(req, "diag/login.html")
