from .payment import *
from .totp import *
from tapiriik.database import db
from tapiriik.sync import Sync
from tapiriik.services import ServiceRecord
from datetime import datetime, timedelta
from bson.objectid import ObjectId

class User:
    def Get(id):
        return db.users.find_one({"_id": ObjectId(id)})
    def Ensure(req):
        if req.user == None:
            req.user = User.Create()
            User.Login(req.user, req)
        return req.user

    def Login(user, req):
        req.session["userid"] = str(user["_id"])
        req.user = user

    def Create():
        uid = db.users.insert({"Created": datetime.utcnow()})  # will mongodb insert an almost empty doc, i.e. _id?
        return db.users.find_one({"_id": uid})

    def GetConnectionRecordsByUser(user):
        return [ServiceRecord(x) for x in db.connections.find({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"]]}})]

    def GetConnectionRecord(user, svcId):
        rec = db.connections.find_one({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"] if x["Service"] == svcId]}})
        return ServiceRecord(rec) if rec else None

    def SetEmail(user, email):
        db.users.update({"_id": ObjectId(user["_id"])}, {"$set": {"Email": email}})

    def AssociatePayment(user, payment):
        db.users.update({"_id": {'$ne': ObjectId(user["_id"])}}, {"$pull": {"Payments": payment}}, multi=True)  # deassociate payment ids from other accounts that may be using them
        db.users.update({"_id": ObjectId(user["_id"])}, {"$addToSet": {"Payments": payment}})
        Sync.ScheduleImmediateSync(user)

    def HasActivePayment(user):
        if "Payments" not in user:
            return False
        for payment in user["Payments"]:
            if payment["Timestamp"] > (datetime.utcnow() - timedelta(days=365.25)):
                return True
        return False

    def ConnectService(user, serviceRecord):
        existingUser = db.users.find_one({"_id": {'$ne': ObjectId(user["_id"])}, "ConnectedServices.ID": ObjectId(serviceRecord._id)})
        if "ConnectedServices" not in user:
            user["ConnectedServices"] = []
        delta = False
        if existingUser is not None:
            # merge merge merge
            user["ConnectedServices"] += existingUser["ConnectedServices"]
            if "Payments" in existingUser:
                if "Payments" not in user:
                    user["Payments"] = []
                user["Payments"] += existingUser["Payments"]
            if "FlowExceptions" in existingUser:
                user["FlowExceptions"] += existingUser["FlowExceptions"]
            user["Email"] = user["Email"] if "Email" in user and user["Email"] is not None else (existingUser["Email"] if "Email" in existingUser else None)
            user["SyncErrorCount"] = (user["SyncErrorCount"] if "SyncErrorCount" in user and user["SyncErrorCount"] is not None else 0) + (existingUser["SyncErrorCount"] if "SyncErrorCount" in existingUser and existingUser["SyncErrorCount"] is not None else 0)
            user["SyncExclusionCount"] = (user["SyncExclusionCount"] if "SyncExclusionCount" in user and user["SyncExclusionCount"] is not None else 0) + (existingUser["SyncExclusionCount"] if "SyncExclusionCount" in existingUser and existingUser["SyncExclusionCount"] is not None else 0)
            user["Created"] = user["Created"] if user["Created"] < existingUser["Created"] else existingUser["Created"]
            if "AncestorAccounts" not in user:
                user["AncestorAccounts"] = []
            user["AncestorAccounts"] += existingUser["AncestorAccounts"] if "AncestorAccounts" in existingUser else []
            user["AncestorAccounts"] += [existingUser["_id"]]
            delta = True
            db.users.remove({"_id": existingUser["_id"]})
        else:
            if serviceRecord._id not in [x["ID"] for x in user["ConnectedServices"]]:
                # we might be connecting a second account for the same service
                for duplicateConn in [x for x in user["ConnectedServices"] if x["Service"] == serviceRecord.Service.ID]:
                    dupeRecord = User.GetConnectionRecord(user, serviceRecord.Service.ID)  # this'll just pick the first connection of type, but we repeat the right # of times anyways
                    Service.DeleteServiceRecord(dupeRecord)
                    User.DisconnectService(dupeRecord, preserveUser=True)  # preserveUser prevents GCing the user immediately before it's needed

                user["ConnectedServices"].append({"Service": serviceRecord.Service.ID, "ID": serviceRecord._id})
                delta = True

        db.users.update({"_id": user["_id"]}, user)
        if delta or (hasattr(serviceRecord, "SyncErrors") and len(serviceRecord.SyncErrors) > 0):  # also schedule an immediate sync if there is an outstanding error (i.e. user reconnected)
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


class SessionAuth:
    def process_request(self, req):
        userId = req.session.get("userid")
        isSU = False
        if req.session.get("substituteUserid") is not None:
            userId = req.session.get("substituteUserid")
            isSU = True

        if userId is None:
            req.user = None
        else:
            req.user = db.users.find_one({"_id": ObjectId(userId)})
            if req.user is not None:
                req.user["Substitute"] = isSU
