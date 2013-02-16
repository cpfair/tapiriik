from tapiriik.database import db
from tapiriik.sync import Sync
from datetime import datetime
from bson.objectid import ObjectId

class User:
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

    def ConnectService(user, serviceRecord):
        existingUser = db.users.find_one({"_id": {'$ne': ObjectId(user["_id"])}, "ConnectedServices.ID": ObjectId(serviceRecord["_id"])})
        if "ConnectedServices" not in user:
            user["ConnectedServices"] = []
        delta = False
        if existingUser is not None:
            # merge merge merge
            user["ConnectedServices"] += existingUser["ConnectedServices"]
            delta = True
            db.users.remove({"_id": existingUser["_id"]})
        else:
            if serviceRecord["_id"] not in [x["ID"] for x in user["ConnectedServices"]]:
                user["ConnectedServices"].append({"Service": serviceRecord["Service"], "ID": serviceRecord["_id"]})
                delta = True
        db.users.update({"_id": user["_id"]}, {"$set": {"ConnectedServices": user["ConnectedServices"]}})
        if delta:
            Sync.ScheduleImmediateSync(user)

    def DisconnectService(user, serviceRecord):
        db.users.update({"_id": user["_id"]}, {"$pull": {"ConnectedServices": {"ID": serviceRecord["_id"]}}})

    def AuthByService(serviceRecord):
        return db.users.find_one({"ConnectedServices.ID": serviceRecord["_id"]})


class SessionAuth:
    def process_request(self, req):
        userId = req.session.get("userid")

        if userId == None:
            req.user = None
        else:
            req.user = db.users.find_one({"_id": ObjectId(userId)})
