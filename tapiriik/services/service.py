from tapiriik.services import *
from .service_record import ServiceRecord
from tapiriik.database import db, cachedb
from bson.objectid import ObjectId

# Really don't know why I didn't make most of this part of the ServiceBase.
class Service:

    # These options are used as the back for all service record's configurations
    _globalConfigurationDefaults = {
        "sync_private": False,
        "allow_activity_flow_exception_bypass_via_self": False,
        "auto_pause": False
    }

    def Init():
        Service._serviceMappings = {x.ID: x for x in Service.List()}
        for svc in Service.List():
            if svc.IDAliases:
                Service._serviceMappings.update({x: svc for x in svc.IDAliases})

    def FromID(id):
        if id in Service._serviceMappings:
            return Service._serviceMappings[id]
        raise ValueError

    def List():
        private_svc_map = {svc.ID: svc for svc in PRIVATE_SERVICES}
        svc_list = (
            private_svc_map.get("garminconnect2"),
            RunKeeper,
            Strava,
            GarminConnect,
            Endomondo,
            SportTracks,
            Dropbox,
            TrainingPeaks,
            RideWithGPS,
            TrainAsONE,
            Pulsstory,
            Motivato,
            NikePlus,
            VeloHero,
            TrainerRoad,
            Smashrun,
            BeginnerTriathlete,
            Setio,
            Singletracker,
            Aerobia,
            private_svc_map.get("runsense")
        )
        return tuple(x for x in svc_list if x is not None)

    def PreferredDownloadPriorityList():
        # Ideally, we'd make an informed decision based on whatever features the activity had
        # ...but that would require either a) downloading it from evry service or b) storing a lot more activity metadata
        # So, I think this will do for now
        return [
            TrainerRoad, # Special case, since TR has a lot more data in some very specific areas
            GarminConnect, # The reference
            Smashrun,  # TODO: not sure if this is the right place, but it seems to have a lot of data
            SportTracks, # Pretty much equivalent to GC, no temperature (not that GC temperature works all thar well now, but I digress)
            TrainingPeaks, # No seperate run cadence, but has temperature
            Dropbox, # Equivalent to any of the above
            RideWithGPS, # Uses TCX for everything, so same as Dropbox
            TrainAsONE,
            VeloHero, # PWX export, no temperature
            Strava, # No laps
            Endomondo, # No laps, no cadence
            RunKeeper, # No laps, no cadence, no power
            BeginnerTriathlete, # No temperature
            Motivato,
            NikePlus,
            Pulsstory,
            Setio,
            Singletracker,
            Aerobia
        ] + PRIVATE_SERVICES

    def WebInit():
        from tapiriik.settings import WEB_ROOT
        from django.core.urlresolvers import reverse
        for itm in Service.List():
            itm.WebInit()
            itm.UserDisconnectURL = WEB_ROOT + reverse("auth_disconnect", kwargs={"service": itm.ID})

    def GetServiceRecordByID(uid):
        return ServiceRecord(db.connections.find_one({"_id": ObjectId(uid)}))

    def EnsureServiceRecordWithAuth(service, uid, authDetails, extendedAuthDetails=None, persistExtendedAuthDetails=False):
        from tapiriik.auth.credential_storage import CredentialStore
        if persistExtendedAuthDetails and not service.RequiresExtendedAuthorizationDetails:
            raise ValueError("Attempting to persist extended auth details on service that doesn't use them")
        # think this entire block could be replaced with an upsert...

        serviceRecord = ServiceRecord(db.connections.find_one({"ExternalID": uid, "Service": service.ID}))
        # Coming out of CredentialStorage these are objects that can't be stuffed into mongodb right away
        # Should really figure out how to mangle pymongo into doing the serialization for me...
        extendedAuthDetailsForStorage = CredentialStore.FlattenShadowedCredentials(extendedAuthDetails) if extendedAuthDetails else None
        if serviceRecord is None:
            db.connections.insert({"ExternalID": uid, "Service": service.ID, "SynchronizedActivities": [], "Authorization": authDetails, "ExtendedAuthorization": extendedAuthDetailsForStorage if persistExtendedAuthDetails else None})
            serviceRecord = ServiceRecord(db.connections.find_one({"ExternalID": uid, "Service": service.ID}))
            serviceRecord.ExtendedAuthorization = extendedAuthDetails # So SubscribeToPartialSyncTrigger can use it (we don't save the whole record after this point)
            if service.PartialSyncTriggerRequiresPolling:
                service.SubscribeToPartialSyncTrigger(serviceRecord) # The subscription is attached more to the remote account than to the local one, so we subscribe/unsubscribe here rather than in User.ConnectService, etc.
        elif serviceRecord.Authorization != authDetails or (hasattr(serviceRecord, "ExtendedAuthorization") and serviceRecord.ExtendedAuthorization != extendedAuthDetailsForStorage):
            db.connections.update({"ExternalID": uid, "Service": service.ID}, {"$set": {"Authorization": authDetails, "ExtendedAuthorization": extendedAuthDetailsForStorage if persistExtendedAuthDetails else None}})

        # if not persisted, these details are stored in the cache db so they don't get backed up
        if service.RequiresExtendedAuthorizationDetails:
            if not persistExtendedAuthDetails:
                cachedb.extendedAuthDetails.update({"ID": serviceRecord._id}, {"ID": serviceRecord._id, "ExtendedAuthorization": extendedAuthDetailsForStorage}, upsert=True)
            else:
                cachedb.extendedAuthDetails.remove({"ID": serviceRecord._id})
        return serviceRecord

    def PersistExtendedAuthDetails(serviceRecord):
        if not serviceRecord.HasExtendedAuthorizationDetails():
            raise ValueError("No extended auth details to persist")
        if serviceRecord.ExtendedAuthorization:
            # Already persisted, nothing to do
            return
        extAuthRecord = cachedb.extendedAuthDetails.find_one({"ID": serviceRecord._id})
        if not extAuthRecord:
            raise ValueError("Service record claims to have extended auth, facts suggest otherwise")
        else:
            extAuth = extAuthRecord["ExtendedAuthorization"]
        db.connections.update({"_id": serviceRecord._id}, {"$set": {"ExtendedAuthorization": extAuth}})
        cachedb.extendedAuthDetails.remove({"ID": serviceRecord._id})

    def DeleteServiceRecord(serviceRecord):
        svc = serviceRecord.Service
        svc.DeleteCachedData(serviceRecord)
        if svc.PartialSyncTriggerRequiresPolling and serviceRecord.PartialSyncTriggerSubscribed:
            svc.UnsubscribeFromPartialSyncTrigger(serviceRecord)
        svc.RevokeAuthorization(serviceRecord)
        cachedb.extendedAuthDetails.remove({"ID": serviceRecord._id})
        db.connections.remove({"_id": serviceRecord._id})

Service.Init()
