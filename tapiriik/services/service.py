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
            print("post" + itm.UserAuthorizationURL)

    def GetServiceRecord(service, authRecord):
        authRecord["Service"] = service.ID
        serviceRecord = db.connections.find_one(authRecord)
        if serviceRecord is None:
            db.connections.insert(authRecord)
            serviceRecord = db.connections.find_one(authRecord)
        return serviceRecord
