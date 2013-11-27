from tapiriik.services import *
from .service_record import ServiceRecord
from tapiriik.database import db, cachedb
from bson.objectid import ObjectId

# Really don't know why I didn't make most of this part of the ServiceBase.
class Service:
    _serviceMappings = {
                        "runkeeper": RunKeeper,
                        "strava": Strava,
                        "endomondo": Endomondo,
                        "dropbox": Dropbox,
                        "garminconnect": GarminConnect,
                        "sporttracks": SportTracks,
                        "rwgps": RideWithGPS
                        }

    # These options are used as the back for all service record's configurations
    _globalConfigurationDefaults = {
        "sync_private": True,
        "allow_activity_flow_exception_bypass_via_self": False
    }

    def FromID(id):
        if id in Service._serviceMappings:
            return Service._serviceMappings[id]
        raise ValueError

    def List():
        return [RunKeeper, Strava, Endomondo, GarminConnect, SportTracks, Dropbox, RideWithGPS]

    def WebInit():
        from tapiriik.settings import WEB_ROOT
        from django.core.urlresolvers import reverse
        for itm in Service.List():
            itm.WebInit()
            itm.UserDisconnectURL = WEB_ROOT + reverse("auth_disconnect", kwargs={"service": itm.ID})

    def GetServiceRecordWithAuthDetails(service, authDetails):
        return ServiceRecord(db.connections.find_one({"Service": service.ID, "Authorization": authDetails}))

    def GetServiceRecordByID(uid):
        return ServiceRecord(db.connections.find_one({"_id": ObjectId(uid)}))

    def EnsureServiceRecordWithAuth(service, uid, authDetails, extendedAuthDetails=None, persistExtendedAuthDetails=False):
        if persistExtendedAuthDetails and not service.RequiresExtendedAuthorizationDetails:
            raise ValueError("Attempting to persist extended auth details on service that doesn't use them")
        # think this entire block could be replaced with an upsert...

        serviceRecord = ServiceRecord(db.connections.find_one({"ExternalID": uid, "Service": service.ID}))
        if serviceRecord is None:
            db.connections.insert({"ExternalID": uid, "Service": service.ID, "SynchronizedActivities": [], "Authorization": authDetails, "ExtendedAuthorization": extendedAuthDetails if persistExtendedAuthDetails else None})
            serviceRecord = ServiceRecord(db.connections.find_one({"ExternalID": uid, "Service": service.ID}))
        elif serviceRecord.Authorization != authDetails or (hasattr(serviceRecord, "ExtendedAuthorization") and serviceRecord.ExtendedAuthorization != extendedAuthDetails):
            db.connections.update({"ExternalID": uid, "Service": service.ID}, {"$set": {"Authorization": authDetails, "ExtendedAuthorization": extendedAuthDetails if persistExtendedAuthDetails else None}})

        # if not persisted, these details are stored in the cache db so they don't get backed up
        if service.RequiresExtendedAuthorizationDetails:
            if not persistExtendedAuthDetails:
                cachedb.extendedAuthDetails.update({"ID": serviceRecord._id}, {"ID": serviceRecord._id, "ExtendedAuthorization": extendedAuthDetails}, upsert=True)
            else:
                cachedb.extendedAuthDetails.remove({"ID": serviceRecord._id})
        return serviceRecord

    def DeleteServiceRecord(serviceRecord):
        svc = serviceRecord.Service
        svc.DeleteCachedData(serviceRecord)
        svc.RevokeAuthorization(serviceRecord)
        cachedb.extendedAuthDetails.remove({"ID": serviceRecord._id})
        db.connections.remove({"_id": serviceRecord._id})
