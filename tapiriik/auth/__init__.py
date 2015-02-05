from tapiriik.payments import *
from .totp import *
from tapiriik.database import db
from tapiriik.sync import Sync
from tapiriik.services import ServiceRecord
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD
from datetime import datetime, timedelta
from pymongo.read_preferences import ReadPreference
from bson.objectid import ObjectId

import copy

class User:
    ConfigurationDefaults = {
        "suppress_auto_sync": False,
        "sync_upload_delay": 0,
        "sync_skip_before": None
    }
    def Get(id):
        return db.users.find_one({"_id": ObjectId(id)})

    def GetByConnection(svcRec):
        return db.users.find_one({"ConnectedServices.ID": svcRec._id})

    def Ensure(req):
        from ipware.ip import get_real_ip
        if req.user == None:
            req.user = User.Create(creationIP=get_real_ip(req))
            User.Login(req.user, req)
        return req.user

    def Login(user, req):
        req.session["userid"] = str(user["_id"])
        req.user = user

    def Logout(req):
        del req.session["userid"]
        del req.user

    def Create(creationIP=None):
        uid = db.users.insert({"Created": datetime.utcnow(), "CreationIP": creationIP})  # will mongodb insert an almost empty doc, i.e. _id?
        return db.users.find_one({"_id": uid}, read_preference=ReadPreference.PRIMARY)

    def GetConnectionRecordsByUser(user):
        return [ServiceRecord(x) for x in db.connections.find({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"]]}})]

    def GetConnectionRecord(user, svcId):
        rec = db.connections.find_one({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"] if x["Service"] == svcId]}})
        return ServiceRecord(rec) if rec else None

    def SetEmail(user, email):
        db.users.update({"_id": ObjectId(user["_id"])}, {"$set": {"Email": email}})

    def SetTimezone(user, tz):
        db.users.update({"_id": ObjectId(user["_id"])}, {"$set": {"Timezone": tz}})

    def _assocPaymentLikeObject(user, collection, payment_like_object, schedule_now, skip_deassoc=False):
        # Since I seem to have taken this duck-typing quite far
        # First, deassociate payment ids from other accounts that may be using them
        if "_id" in payment_like_object and not skip_deassoc:
            db.users.update({}, {"$pull": {collection: {"_id": payment_like_object["_id"]}}}, multi=True)
        # Then, attach to us
        db.users.update({"_id": ObjectId(user["_id"])}, {"$addToSet": {collection: payment_like_object}})
        if schedule_now:
            Sync.ScheduleImmediateSync(user)

    def AssociatePayment(user, payment, schedule_now=True):
        User._assocPaymentLikeObject(user, "Payments", payment, schedule_now)

    def AssociateExternalPayment(user, external_payment, schedule_now=False, skip_deassoc=False):
        User._assocPaymentLikeObject(user, "ExternalPayments", external_payment, schedule_now, skip_deassoc)

    def AssociatePromo(user, promo, schedule_now=True):
        User._assocPaymentLikeObject(user, "Promos", promo, schedule_now)

    def HasActivePayment(user):
        # Payments and Promos share the essential data field - Expiry
        # We don't really care if the payment has yet to take place yet - why would it be in the system then?
        # (Timestamp too, but the fact we rely on it here is only for backwards compatability with some old payment records)
        payment_like_objects = (user["Payments"] if "Payments" in user else []) + (user["Promos"] if "Promos" in user else []) + (user["ExternalPayments"] if "ExternalPayments" in user else [])
        for payment in payment_like_objects:
            if "Expiry" in payment:
                if payment["Expiry"] == None or payment["Expiry"] > datetime.utcnow():
                    return True
            else:
                if payment["Timestamp"] > (datetime.utcnow() - timedelta(days=365.25)):
                    return True
        return False

    def PaidUserMongoQuery():
        # Don't need the no-expiry case here, those payments have all expired by now
        return {
            "$or": [
                {"Payments.Expiry": {"$gt": datetime.utcnow()}},
                {"Promos.Expiry": {"$gt": datetime.utcnow()}},
                {"Promos.Expiry": {"$type": 10, "$exists": True}} # === null
            ]
        }

    def IsServiceConnected(user, service_id):
        return service_id in [x["Service"] for x in user["ConnectedServices"]]

    def ConnectService(user, serviceRecord):
        from tapiriik.services import Service, UserExceptionType
        existingUser = db.users.find_one({"_id": {'$ne': ObjectId(user["_id"])}, "ConnectedServices.ID": ObjectId(serviceRecord._id)})
        if "ConnectedServices" not in user:
            user["ConnectedServices"] = []
        delta = False
        if existingUser is not None:
            # merge merge merge

            # Don't let the user end up with two services of the same type, ever
            # It's not fully supported, plus it's caused all sorts of trauma in the past.
            # Note that this will discard the new serviceRecord connection if an existing one exists on the other account
            # ...which isn't the end of the world, compared to screwing around asking the user which they wanted to keep.
            for to_merge_service in existingUser["ConnectedServices"]:
                if len([x for x in user["ConnectedServices"] if x["Service"] == to_merge_service["Service"]]) == 0:
                    user["ConnectedServices"].append(to_merge_service)

            # There's got to be some 1-liner to do this merge
            if "Payments" in existingUser:
                if "Payments" not in user:
                    user["Payments"] = []
                user["Payments"] += existingUser["Payments"]
            if "Promos" in existingUser:
                if "Promos" not in user:
                    user["Promos"] = []
                user["Promos"] += existingUser["Promos"]
            if "ExternalPayments" in existingUser:
                if "ExternalPayments" not in user:
                    user["ExternalPayments"] = []
                user["ExternalPayments"] += existingUser["ExternalPayments"]
            if "FlowExceptions" in existingUser:
                if "FlowExceptions" not in user:
                    user["FlowExceptions"] = []
                user["FlowExceptions"] += existingUser["FlowExceptions"]
            user["Email"] = user["Email"] if "Email" in user and user["Email"] is not None else (existingUser["Email"] if "Email" in existingUser else None)
            user["NonblockingSyncErrorCount"] = (user["NonblockingSyncErrorCount"] if "NonblockingSyncErrorCount" in user and user["NonblockingSyncErrorCount"] is not None else 0) + (existingUser["NonblockingSyncErrorCount"] if "NonblockingSyncErrorCount" in existingUser and existingUser["NonblockingSyncErrorCount"] is not None else 0)
            user["BlockingSyncErrorCount"] = (user["BlockingSyncErrorCount"] if "BlockingSyncErrorCount" in user and user["BlockingSyncErrorCount"] is not None else 0) + (existingUser["BlockingSyncErrorCount"] if "BlockingSyncErrorCount" in existingUser and existingUser["BlockingSyncErrorCount"] is not None else 0)
            user["SyncExclusionCount"] = (user["SyncExclusionCount"] if "SyncExclusionCount" in user and user["SyncExclusionCount"] is not None else 0) + (existingUser["SyncExclusionCount"] if "SyncExclusionCount" in existingUser and existingUser["SyncExclusionCount"] is not None else 0)
            user["Created"] = user["Created"] if user["Created"] < existingUser["Created"] else existingUser["Created"]
            if "AncestorAccounts" not in user:
                user["AncestorAccounts"] = []
            user["AncestorAccounts"] += existingUser["AncestorAccounts"] if "AncestorAccounts" in existingUser else []
            user["AncestorAccounts"] += [existingUser["_id"]]
            user["Timezone"] = user["Timezone"] if "Timezone" in user and user["Timezone"] else (existingUser["Timezone"] if "Timezone" in existingUser else None)
            user["CreationIP"] = user["CreationIP"] if "CreationIP" in user and user["CreationIP"] else (existingUser["CreationIP"] if "CreationIP" in existingUser else None)
            existing_config = existingUser["Config"] if "Config" in existingUser else {}
            existing_config.update(user["Config"] if "Config" in user else {})
            user["Config"] = existing_config
            delta = True
            db.users.remove({"_id": existingUser["_id"]})
        else:
            if serviceRecord._id not in [x["ID"] for x in user["ConnectedServices"]]:
                # we might be connecting a second account for the same service
                for duplicateConn in [x for x in user["ConnectedServices"] if x["Service"] == serviceRecord.Service.ID]:
                    dupeRecord = User.GetConnectionRecord(user, serviceRecord.Service.ID)  # this'll just pick the first connection of type, but we repeat the right # of times anyways
                    Service.DeleteServiceRecord(dupeRecord)
                    # We used to call DisconnectService() here, but the results of that call were getting overwritten, which was unfortunate.
                    user["ConnectedServices"] = [x for x in user["ConnectedServices"] if x["Service"] != serviceRecord.Service.ID]

                user["ConnectedServices"].append({"Service": serviceRecord.Service.ID, "ID": serviceRecord._id})
                delta = True

        db.users.update({"_id": user["_id"]}, user)
        if delta or (hasattr(serviceRecord, "SyncErrors") and len(serviceRecord.SyncErrors) > 0):  # also schedule an immediate sync if there is an outstanding error (i.e. user reconnected)
            db.connections.update({"_id": serviceRecord._id}, {"$pull": {"SyncErrors": {"UserException.Type": UserExceptionType.Authorization}}}) # Pull all auth-related errors from the service so they don't continue to see them while the sync completes.
            Sync.SetNextSyncIsExhaustive(user, True)  # exhaustive, so it'll pick up activities from newly added services / ones lost during an error
            if hasattr(serviceRecord, "SyncErrors") and len(serviceRecord.SyncErrors) > 0:
                Sync.ScheduleImmediateSync(user)

    def DisconnectService(serviceRecord, preserveUser=False):
        # not that >1 user should have this connection
        activeUsers = list(db.users.find({"ConnectedServices.ID": serviceRecord._id}))
        if len(activeUsers) == 0:
            raise Exception("No users found with service " + serviceRecord._id)
        db.users.update({}, {"$pull": {"ConnectedServices": {"ID": serviceRecord._id}}}, multi=True)
        if not preserveUser:
            for user in activeUsers:
                if len(user["ConnectedServices"]) - 1 == 0:
                    # I guess we're done here?
                    db.users.remove({"_id": user["_id"]})

    def AuthByService(serviceRecord):
        return db.users.find_one({"ConnectedServices.ID": serviceRecord._id})

    def SetFlowException(user, sourceServiceRecord, targetServiceRecord, flowToTarget=True, flowToSource=True):
        if "FlowExceptions" not in user:
            user["FlowExceptions"] = []

        # flow exceptions are stored in "forward" direction - service-account X will not send activities to service-account Y
        forwardException = {"Target": {"Service": targetServiceRecord.Service.ID, "ExternalID": targetServiceRecord.ExternalID}, "Source": {"Service": sourceServiceRecord.Service.ID, "ExternalID": sourceServiceRecord.ExternalID}}
        backwardsException = {"Target": forwardException["Source"], "Source": forwardException["Target"]}
        if flowToTarget is not None:
            if flowToTarget:
                user["FlowExceptions"][:] = [x for x in user["FlowExceptions"] if x != forwardException]
            elif not flowToTarget and forwardException not in user["FlowExceptions"]:
                user["FlowExceptions"].append(forwardException)
        if flowToSource is not None:
            if flowToSource:
                user["FlowExceptions"][:] = [x for x in user["FlowExceptions"] if x != backwardsException]
            elif not flowToSource and backwardsException not in user["FlowExceptions"]:
                user["FlowExceptions"].append(backwardsException)
        db.users.update({"_id": user["_id"]}, {"$set": {"FlowExceptions": user["FlowExceptions"]}})

    def GetFlowExceptions(user):
        if "FlowExceptions" not in user:
            return {}
        return user["FlowExceptions"]

    def CheckFlowException(user, sourceServiceRecord, targetServiceRecord):
        ''' returns true if there is a flow exception blocking activities moving from source to destination '''
        forwardException = {"Target": {"Service": targetServiceRecord.Service.ID, "ExternalID": targetServiceRecord.ExternalID}, "Source": {"Service": sourceServiceRecord.Service.ID, "ExternalID": sourceServiceRecord.ExternalID}}
        return "FlowExceptions" in user and forwardException in user["FlowExceptions"]

    # You may recognize that these functions are shamelessly copy-pasted from service_base.py
    def GetConfiguration(user):
        config = copy.deepcopy(User.ConfigurationDefaults)
        config.update(user["Config"] if "Config" in user else {})
        return config

    def SetConfiguration(user, config, no_save=False, drop_existing=False):
        sparseConfig = {}
        if not drop_existing:
            sparseConfig = copy.deepcopy(User.GetConfiguration(user))
        sparseConfig.update(config)

        keys_to_delete = []
        for k, v in sparseConfig.items():
            if (k in User.ConfigurationDefaults and User.ConfigurationDefaults[k] == v):
                keys_to_delete.append(k)  # it's the default, we can not store it
        for k in keys_to_delete:
            del sparseConfig[k]
        user["Config"] = sparseConfig
        if not no_save:
            db.users.update({"_id": user["_id"]}, {"$set": {"Config": sparseConfig}})


class DiagnosticsUser:
    def IsAuthenticated(req):
        return DIAG_AUTH_TOTP_SECRET is None or DIAG_AUTH_PASSWORD is None or ("diag_auth" in req.session and req.session["diag_auth"] is True)

    def Authorize(req):
        req.session["diag_auth"] = True

class SessionAuth:
    def process_request(self, req):
        userId = req.session.get("userid")
        isSU = False
        if req.session.get("substituteUserid") is not None or ("su" in req.GET and DiagnosticsUser.IsAuthenticated(req)):
            userId = req.GET["su"] if "su" in req.GET else req.session.get("substituteUserid")
            isSU = True

        if userId is None:
            req.user = None
        else:
            req.user = db.users.find_one({"_id": ObjectId(userId)})
            if req.user is not None:
                req.user["Config"] = User.GetConfiguration(req.user) # Populate defaults
                req.user["Substitute"] = isSU
