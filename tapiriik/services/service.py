from tapiriik.services import *
from tapiriik.database import db

class Service:
    def FromID(id):
        if id=="runkeeper":
            return RunKeeper
        elif id=="strava":
            return Strava
        raise ValueError

    def List():
        return [RunKeeper, Strava]

    def WebInit():
        global UserAuthorizationURL
        for itm in Service.List():
            itm.WebInit()

    def GetServiceRecordWithAuthDetails(service, authDetails):
        return db.connections.find_one({"Service": service.ID, "Authorization": authDetails})

    def EnsureServiceRecordWithAuth(service, uid, authDetails):
        serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        if serviceRecord is None:
            db.connections.insert({"ExternalID": uid, "Service": service.ID, "SynchronizedActivities": [], "Authorization": authDetails})
            serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        if serviceRecord["Authorization"] != authDetails:
            db.connections.update({"ExternalID": uid, "Service": service.ID}, {"$set": {"Authorization": authDetails}})
        return serviceRecord
