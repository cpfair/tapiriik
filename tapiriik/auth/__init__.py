from .payment import *
from .totp import *
from tapiriik.database import db
from tapiriik.sync import Sync
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
        return list(db.connections.find({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"]]}}))

    def GetConnectionRecord(user, svcId):
        return db.connections.find_one({"_id": {"$in": [x["ID"] for x in user["ConnectedServices"] if x["Service"] == svcId]}})

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
        existingUser = db.users.find_one({"_id": {'$ne': ObjectId(user["_id"])}, "ConnectedServices.ID": ObjectId(serviceRecord["_id"])})
        if "ConnectedServices" not in user:
            user["ConnectedServices"] = []
        delta = False
        if existingUser is not None:
            # merge merge merge
            user["ConnectedServices"] += existingUser["ConnectedServices"]
            if "Payments" in existingUser:
                user["Payments"] += existingUser["Payments"]
            if "FlowExceptions" in existingUser:
                user["FlowExceptions"] += existingUser["FlowExceptions"]
            delta = True
            db.users.remove({"_id": existingUser["_id"]})
        else:
            if serviceRecord["_id"] not in [x["ID"] for x in user["ConnectedServices"]]:
                user["ConnectedServices"].append({"Service": serviceRecord["Service"], "ID": serviceRecord["_id"]})
                delta = True
        db.users.update({"_id": user["_id"]}, {"$set": {"ConnectedServices": user["ConnectedServices"]}})
        if delta or ("SyncErrors" in serviceRecord and len(serviceRecord["SyncErrors"]) > 0):  # also schedule an immediate sync if there is an outstanding error (i.e. user reconnected)
            Sync.SetNextSyncIsExhaustive(user, True)  # exhaustive, so it'll pick up activities from newly added services / ones lost during an error
            if "SyncErrors" in serviceRecord and len(serviceRecord["SyncErrors"]) > 0:
                Sync.ScheduleImmediateSync(user)

    def DisconnectService(serviceRecord):
        # not that >1 user should have this connection
        activeUsers = list(db.users.find({"ConnectedServices.ID": serviceRecord["_id"]}))
        if len(activeUsers) == 0:
            raise Exception("No users found with service " + serviceRecord["_id"])
        db.users.update({}, {"$pull": {"ConnectedServices": {"ID": serviceRecord["_id"]}}}, multi=True)
        for user in activeUsers:
            if len(user["ConnectedServices"]) - 1 == 0:
                # I guess we're done here?
                db.users.remove({"_id": user["_id"]})

    def AuthByService(serviceRecord):
        return db.users.find_one({"ConnectedServices.ID": serviceRecord["_id"]})

    def SetFlowException(user, sourceServiceRecord, targetServiceRecord, flowToTarget=True, flowtoSource=True):
        if "FlowExceptions" not in user:
            user["FlowExceptions"] = []

        # flow exceptions are stored in "forward" direction - service-account X will not send activities to service-account Y
        forwardException = {"Target": {"Service": targetServiceRecord["Service"], "ExternalID": targetServiceRecord["ExternalID"]}, "Source": {"Service": sourceServiceRecord["Service"], "ExternalID": sourceServiceRecord["ExternalID"]}}
        backwardsException = {"Target": forwardException["Source"], "Source": forwardException["Target"]}
        if flowToTarget is not None:
            if flowToTarget:
                user["FlowExceptions"][:] = [x for x in user["FlowExceptions"] if x != forwardException]
            elif not flowToTarget and forwardException not in user["FlowExceptions"]:
                user["FlowExceptions"].append(forwardException)
        if flowtoSource is not None:
            if flowtoSource:
                user["FlowExceptions"][:] = [x for x in user["FlowExceptions"] if x != backwardsException]
            elif not flowtoSource and backwardsException not in user["FlowExceptions"]:
                user["FlowExceptions"].append(backwardsException)
        db.users.update({"_id": user["_id"]}, {"$set": {"FlowExceptions": user["FlowExceptions"]}})

    def GetFlowExceptions(user):
        if "FlowExceptions" not in user:
            return {}
        return user["FlowExceptions"]

    def CheckFlowException(user, sourceServiceRecord, targetServiceRecord):
        ''' returns true if there is a flow exception blocking activities moving from source to destination '''
        forwardException = {"Target": {"Service": targetServiceRecord["Service"], "ExternalID": targetServiceRecord["ExternalID"]}, "Source": {"Service": sourceServiceRecord["Service"], "ExternalID": sourceServiceRecord["ExternalID"]}}
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
            req.user["Substitute"] = isSU
