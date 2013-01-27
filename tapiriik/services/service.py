#reinventing the interface here
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

    def EnsureServiceRecordWithAuthDetails(service, uid, authDetails):
        serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        if serviceRecord is None:
            db.connections.insert({"ExternalID": uid, "Service": service.ID, "Authorization": authDetails})
            serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        return serviceRecord
