from tapiriik.database import db
from datetime import datetime
from bson.objectid import ObjectId

class User:
    def Ensure(req):
        if req.user == None:
            req.user = User.Create()
            User.Login(req.user, req)

    def Login(user, req):
        req.session["userid"] = str(user["_id"])
        req.user = user

    def Create():
        uid = db.users.insert({"Created": datetime.utcnow()})  # will mongodb insert an almost empty doc, i.e. _id?
        return db.users.find_one({"_id": uid})

    def ConnectService(user, serviceRecord):
        existingUser = db.users.find_one({"_id": {'$ne': user["_id"]}, "ConnectedServices": {'$in': [serviceRecord["_id"]]}})
        if "ConnectedServices" not in user:
            user["ConnectedServices"] = []
        if existingUser is not None:
            # merge merge merge
            user["ConnectedServices"] += existingUser.ConnectedServices
            db.users.delete({"_id": existingUser["_id"]})
        else:
            if serviceRecord["_id"] not in user["ConnectedServices"]:
                user["ConnectedServices"].append(serviceRecord["_id"])
        db.users.update({"_id": user["_id"]}, {"$set":{"ConnectedServices": user["ConnectedServices"]}})

    def AuthByService(serviceRecord):
        return db.users.find_one({"ConnectedServices": {'$in': [serviceRecord["_id"]]}})





class SessionAuth:
    def process_request(self, req):
        userId = req.session.get("userid")

        if userId == None:
            req.user = None
        else:
            req.user = db.users.find_one({"_id": ObjectId(userId)})
