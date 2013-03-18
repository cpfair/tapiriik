from tapiriik.services import *
from tapiriik.database import db
import copy

class Service:
    _serviceMappings = {"runkeeper": RunKeeper,
                        "strava": Strava,
                        "endomondo": Endomondo,
                        "dropbox": Dropbox}

    def FromID(id):
        if id in Service._serviceMappings:
            return Service._serviceMappings[id]
        raise ValueError

    def List():
        return [RunKeeper, Strava, Endomondo, Dropbox]

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

    def _mergeConfig(base, config):
        return dict(list(base.items()) + list(config.items()))

    def HasConfiguration(svcRec):
        if not Service.FromID(svcRec["Service"]).Configurable:
            return False  # of course not
        return "Config" in svcRec and len(svcRec["Config"].values()) > 0

    def GetConfiguration(svcRec):
        svc = Service.FromID(svcRec["Service"])
        if not svc.Configurable:
            raise ValueError("Passed service is not configurable")
        return Service._mergeConfig(svc.ConfigurationDefaults, svcRec["Config"]) if "Config" in svcRec else svc.ConfigurationDefaults

    def SetConfiguration(config, svcRec):
        sparseConfig = copy.deepcopy(config)
        svc = Service.FromID(svcRec["Service"])
        svc.ConfigurationUpdating(svcRec, config, Service.GetConfiguration(svcRec))
        for k, v in config.items():
            if k in svc.ConfigurationDefaults and svc.ConfigurationDefaults[k] == v:
                del sparseConfig[k]  # it's the default, we can not store it
        db.connections.update({"_id": svcRec["_id"]}, {"$set": {"Config": sparseConfig}})
