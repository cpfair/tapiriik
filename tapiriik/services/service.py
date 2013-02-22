from tapiriik.services import *
from tapiriik.database import db


class Service:
    _serviceMappings = {"runkeeper": RunKeeper,
                        "strava": Strava,
                        "endomondo": Endomondo}

    def FromID(id):
        if id in Service._serviceMappings:
            return Service._serviceMappings[id]
        raise ValueError

    def List():
        return [RunKeeper, Strava, Endomondo]

    def WebInit():
        from tapiriik.settings import WEB_ROOT
        from django.core.urlresolvers import reverse
        for itm in Service.List():
            itm.WebInit()
            itm.UserDisconnectURL = WEB_ROOT + reverse("auth_disconnect", kwargs={"service": itm.ID})

    def GetServiceRecordWithAuthDetails(service, authDetails):
        return db.connections.find_one({"Service": service.ID, "Authorization": authDetails})

    def GetServiceRecordByID(uid):
        return db.connections.find_one({"_id": uid})

    def EnsureServiceRecordWithAuth(service, uid, authDetails):
        serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        if serviceRecord is None:
            db.connections.insert({"ExternalID": uid, "Service": service.ID, "SynchronizedActivities": [], "Authorization": authDetails})
            serviceRecord = db.connections.find_one({"ExternalID": uid, "Service": service.ID})
        if serviceRecord["Authorization"] != authDetails:
            db.connections.update({"ExternalID": uid, "Service": service.ID}, {"$set": {"Authorization": authDetails}})
        return serviceRecord

    def DeleteServiceRecord(serviceRecord):
        svc = Service.FromID(serviceRecord["Service"])
        svc.DeleteCachedData(serviceRecord)
        svc.RevokeAuthorization(serviceRecord)
        db.connections.remove({"_id": serviceRecord["_id"]})
