from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

from django.core.urlresolvers import reverse
import pytz
from datetime import datetime, timedelta
import requests
import json
import os

import logging
logger = logging.getLogger(__name__)

class GarminConnectService(ServiceBase):
    ID = "garminconnect"
    DisplayName = "Garmin Connect"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    _activityMappings = {
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain_biking": ActivityType.MountainBiking,
                                "walking": ActivityType.Walking,
                                "hiking": ActivityType.Hiking,
                                "resort_skiing_snowboarding": ActivityType.DownhillSkiing,
                                "cross_country_skiing": ActivityType.CrossCountrySkiing,
                                "backcountry_skiing_snowboarding": ActivityType.CrossCountrySkiing,  # ish
                                "skating": ActivityType.Skating,
                                "swimming": ActivityType.Swimming,
                                "rowing": ActivityType.Rowing,
                                "elliptical": ActivityType.Elliptical,
                                "all": ActivityType.Other  # everything will eventually resolve to this
    }

    _reverseActivityMappings = {  # Removes ambiguities when mapping back to their activity types
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain_biking": ActivityType.MountainBiking,
                                "walking": ActivityType.Walking,
                                "hiking": ActivityType.Hiking,
                                "resort_skiing_snowboarding": ActivityType.DownhillSkiing,
                                "cross_country_skiing": ActivityType.CrossCountrySkiing,
                                "skating": ActivityType.Skating,
                                "swimming": ActivityType.Swimming,
                                "rowing": ActivityType.Rowing,
                                "elliptical": ActivityType.Elliptical,
                                "other": ActivityType.Other  # I guess? (vs. "all" that is)
    }

    SupportedActivities = list(_activityMappings.values())

    SupportsHR = SupportsCadence = True

    _sessionCache = SessionCache(lifetime=timedelta(minutes=30), freshen_on_get=True)

    def __init__(self):
        self._activityHierarchy = requests.get("http://connect.garmin.com/proxy/activity-service-1.2/json/activity_types").json()["dictionary"]

    def _get_cookies(self, record=None, email=None, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
        params = {"login": "login", "login:loginUsernameField": email, "login:password": password, "login:signInButton": "Sign In", "javax.faces.ViewState": "j_id1"}
        preResp = requests.get("https://connect.garmin.com/signin")
        resp = requests.post("https://connect.garmin.com/signin", data=params, allow_redirects=False, cookies=preResp.cookies)
        if resp.status_code >= 500 and resp.status_code<600:
            raise APIException("Remote API failure")
        if resp.status_code != 302:  # yep
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        if record:
            self._sessionCache.Set(record.ExternalID, preResp.cookies)
        return preResp.cookies

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        cookies = self._get_cookies(email=email, password=password)
        username = requests.get("http://connect.garmin.com/user/username", cookies=cookies).json()["username"]
        if not len(username):
            raise APIException("Unable to retrieve username", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (username, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})


    def _resolveActivityType(self, act_type):
        # Mostly there are two levels of a hierarchy, so we don't really need this as the parent is included in the listing.
        # But maybe they'll change that some day?
        while act_type not in self._activityMappings:
            try:
                act_type = [x["parent"]["key"] for x in self._activityHierarchy if x["key"] == act_type][0]
            except IndexError:
                raise ValueError("Activity type not found in activity hierarchy")
        return self._activityMappings[act_type]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        #http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities?&start=0&limit=50
        cookies = self._get_cookies(record=serviceRecord)
        page = 1
        pageSz = 50
        activities = []
        exclusions = []
        while True:
            logger.debug("Req with " + str({"start": (page - 1) * pageSz, "limit": pageSz}))
            res = requests.get("http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities", params={"start": (page - 1) * pageSz, "limit": pageSz}, cookies=cookies)
            res = res.json()["results"]
            if "activities" not in res:
                break  # No activities on this page - empty account.
            for act in res["activities"]:
                act = act["activity"]
                if "sumDistance" not in act:
                    exclusions.append(APIExcludeActivity("No distance", activityId=act["activityId"]))
                    continue
                activity = UploadedActivity()

                try:
                    activity.TZ = pytz.timezone(act["activityTimeZone"]["key"])
                except pytz.exceptions.UnknownTimeZoneError:
                    activity.TZ = pytz.FixedOffset(float(act["activityTimeZone"]["offset"]) * 60)

                logger.debug("Name " + act["activityName"]["value"] + ":")
                if len(act["activityName"]["value"].strip()) and act["activityName"]["value"] != "Untitled":
                    activity.Name = act["activityName"]["value"]
                # beginTimestamp/endTimestamp is in UTC
                activity.StartTime = pytz.utc.localize(datetime.utcfromtimestamp(float(act["beginTimestamp"]["millis"])/1000))
                if "sumElapsedDuration" in act:
                    activity.EndTime = activity.StartTime + timedelta(0, round(float(act["sumElapsedDuration"]["value"])))
                elif "sumDuration" in act:
                    activity.EndTime = activity.StartTime + timedelta(minutes=float(act["sumDuration"]["minutesSeconds"].split(":")[0]), seconds=float(act["sumDuration"]["minutesSeconds"].split(":")[1]))
                else:
                    activity.EndTime = pytz.utc.localize(datetime.utcfromtimestamp(float(act["endTimestamp"]["millis"])/1000))
                logger.debug("Activity s/t " + str(activity.StartTime) + " on page " + str(page))
                activity.AdjustTZ()
                # TODO: fix the distance stats to account for the fact that this incorrectly reported km instead of meters for the longest time.
                activity.Stats.Distance = float(act["sumDistance"]["value"]) * (1.60934 if act["sumDistance"]["uom"] == "mile" else 1) * 1000  # In meters...
                activity.Type = self._resolveActivityType(act["activityType"]["key"])

                activity.CalculateUID()
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["activityId"]}]
                activities.append(activity)
            logger.debug("Finished page " + str(page) + " of " + str(res["search"]["totalPages"]))
            if not exhaustive or int(res["search"]["totalPages"]) == page:
                break
            else:
                page += 1
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        #http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/#####?full=true
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        cookies = self._get_cookies(record=serviceRecord)
        res = requests.get("http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/" + str(activityID) + "?full=true", cookies=cookies)
        try:
            TCXIO.Parse(res.content, activity)
        except ValueError as e:
            raise APIExcludeActivity("TCX parse error " + str(e))

        return activity

    def UploadActivity(self, serviceRecord, activity):
        #/proxy/upload-service-1.1/json/upload/.tcx
        activity.EnsureTZ()
        tcx_file = TCXIO.Dump(activity)
        files = {"data": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".tcx", tcx_file)}
        cookies = self._get_cookies(record=serviceRecord)
        res = requests.post("http://connect.garmin.com/proxy/upload-service-1.1/json/upload/.tcx", files=files, cookies=cookies)
        res = res.json()["detailedImportResult"]

        if len(res["successes"]) != 1:
            raise APIException("Unable to upload activity")
        actid = res["successes"][0]["internalId"]

        if activity.Type not in [ActivityType.Running, ActivityType.Cycling, ActivityType.Other]:
            # Set the legit activity type - whatever it is, it's not supported by the TCX schema
            acttype = [k for k, v in self._reverseActivityMappings.items() if v == activity.Type]
            if len(acttype) == 0:
                raise APIWarning("GarminConnect does not support activity type " + activity.Type)
            else:
                acttype = acttype[0]
            res = requests.post("http://connect.garmin.com/proxy/activity-service-1.2/json/type/" + str(actid), data={"value": acttype}, cookies=cookies)
            res = res.json()
            if "activityType" not in res or res["activityType"]["key"] != acttype:
                raise APIWarning("Unable to set activity type")



    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
