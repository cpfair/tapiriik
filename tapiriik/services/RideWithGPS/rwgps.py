import os
import math
from datetime import datetime, timedelta

import pytz
import requests
from django.core.urlresolvers import reverse

from tapiriik.settings import WEB_ROOT, RWGPS_APIKEY
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

import logging
logger = logging.getLogger(__name__)

class RideWithGPSService(ServiceBase):
    ID = "rwgps"
    DisplayName = "Ride With GPS"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    # RWGPS does has a "recreation_types" list, but it is not actually used anywhere (yet)
    # (This is a subset of the things returned by that list for future reference...)
    _activityMappings = {
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain biking": ActivityType.MountainBiking,
                                "Hiking": ActivityType.Hiking,
                                "all": ActivityType.Other  # everything will eventually resolve to this
    }
    
    SupportedActivities = list(_activityMappings.values())

    SupportsHR = SupportsCadence = True

    _sessionCache = SessionCache(lifetime=timedelta(minutes=30), freshen_on_get=True)

    def __init__(self):
        pass
        
    def _add_auth_params(self, params=None, record=None):
        """
        Adds apikey and authorization (email/password) to the passed-in params,
        returns modified params dict.
        """
        from tapiriik.auth.credential_storage import CredentialStore
        if params is None:
            params = {}
        params['apikey'] = RWGPS_APIKEY
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
            params['email'] = email
            params['password'] = password
            
        return params
    
    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        res = requests.get("https://ridewithgps.com/users/current.json",
                           params={'email': email, 'password': password, 'apikey': RWGPS_APIKEY})
        res.raise_for_status()
        res = res.json()
        member_id = res["user"]["id"]
        if not member_id:
            raise APIException("Unable to retrieve id", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (member_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def _duration_to_seconds(self, s):
        """
        Converts a duration in form HH:MM:SS to number of seconds for use in timedelta construction.
        """
        hours, minutes, seconds = (["0", "0"] + s.split(":"))[-3:]
        hours = int(hours)
        minutes = int(minutes)
        seconds = float(seconds)
        total_seconds = int(hours + 60000 * minutes + 1000 * seconds)
        return total_seconds
    
    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        # http://ridewithgps.com/users/1/trips.json?limit=200&order_by=created_at&order_dir=asc
        # offset also supported
        page = 1
        pageSz = 50
        activities = []
        exclusions = []
        while True:
            logger.debug("Req with " + str({"start": (page - 1) * pageSz, "limit": pageSz}))
            
            params = {"offset": (page - 1) * pageSz, "limit": pageSz}
            params = self._add_auth_params(params, record=serviceRecord)
            
            res = requests.get("http://ridewithgps.com/users/{}/trips.json".format(serviceRecord.ExternalID), params=params)
            res = res.json()
            total_pages = math.ceil(int(res["results_count"]) / pageSz)
            for act in res["results"]:
                if "first_lat" not in act or "last_lat" not in act:
                    exclusions.append(APIExcludeActivity("No points", activityId=act["activityId"]))
                    continue
                if "distance" not in act:
                    exclusions.append(APIExcludeActivity("No distance", activityId=act["activityId"]))
                    continue
                activity = UploadedActivity()

                activity.TZ = pytz.timezone(act["time_zone"])
                
                logger.debug("Name " + act["name"] + ":")
                if len(act["name"].strip()):
                    activity.Name = act["name"]
                
                activity.StartTime = pytz.utc.localize(datetime.strptime(act["departed_at"], "%Y-%m-%dT%H:%M:%SZ"))
                activity.EndTime = activity.StartTime + timedelta(seconds=self._duration_to_seconds(act["duration"]))
                logger.debug("Activity s/t " + str(activity.StartTime) + " on page " + str(page))
                activity.AdjustTZ()
                
                activity.Distance = float(act["distance"])  # This value is already in meters...
                # Activity type is not implemented yet in RWGPS results; we will assume cycling, though perhaps "OTHER" wouuld be correct
                activity.Type = ActivityType.Cycling

                activity.CalculateUID()
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["id"]}]
                activities.append(activity)
            logger.debug("Finished page {} of {}".format(page, total_pages))
            if not exhaustive or total_pages == page:
                break
            else:
                page += 1
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        #https://ridewithgps.com/trips/??????.gpx
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        res = requests.get("https://ridewithgps.com/trips/{}.tcx".format(activityID),
                           params=self._add_auth_params({'sub_format': 'history'}, record=serviceRecord))
        try:
            TCXIO.Parse(res.content, activity)
        except ValueError as e:
            raise APIExcludeActivity("TCX parse error " + str(e))
        
        return activity

    def UploadActivity(self, serviceRecord, activity):
        #https://ridewithgps.com/trips.json
        activity.EnsureTZ()
        tcx_file = TCXIO.Dump(activity)
        #fp = StringIO(tcx_file)
        #fp.seek(0)
        files = {"data_file": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".tcx", tcx_file)}
        
        params = {}
        params['trip[name]'] = activity.Name
        params['trip[visibility]'] = 1 if activity.Private else 0 # Yes, this logic seems backwards but it's how it works
        
        res = requests.post("https://ridewithgps.com/trips.json", files=files,
                            params=self._add_auth_params(params, record=serviceRecord))
        res.raise_for_status()
        res = res.json()
        if res["success"] != 1:
            raise APIException("Unable to upload activity")


    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
