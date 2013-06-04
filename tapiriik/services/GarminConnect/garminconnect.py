from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException, APIWarning, APIExcludeActivity
from tapiriik.services.tcx import TCXIO

from django.core.urlresolvers import reverse
import pytz
from datetime import datetime, timedelta
import requests
import json
import os

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

    def __init__(self):
        self._activityHierarchy = requests.get("http://connect.garmin.com/proxy/activity-service-1.2/json/activity_types").json()["dictionary"]

    def _get_cookies(self, email, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if password is None:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(email.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(email.ExtendedAuthorization["Email"])
        params = {"login": "login", "login:loginUsernameField": email, "login:password": password, "login:signInButton": "Sign In", "javax.faces.ViewState": "j_id1"}
        preResp = requests.get("https://connect.garmin.com/signin")
        resp = requests.post("https://connect.garmin.com/signin", data=params, allow_redirects=False, cookies=preResp.cookies)
        if resp.status_code != 302:  # yep
            raise APIAuthorizationException("Invalid login")
        return preResp.cookies

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        cookies = self._get_cookies(email, password)
        username = requests.get("https://connect.garmin.com/user/username", cookies=cookies).json()["username"]
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
        cookies = self._get_cookies(serviceRecord)
        page = 1
        pageSz = 50
        activities = []
        exclusions = []
        while True:
            res = requests.get("http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities", data={"start": (page - 1) * pageSz, "limit": pageSz}, cookies=cookies)
            res = res.json()["results"]
            if "activities" not in res:
                break  # No activities on this page - empty account.
            for act in res["activities"]:
                act = act["activity"]
                if "beginLatitude" not in act or "endLatitude" not in act or (act["beginLatitude"] is act["endLatitude"] and act["beginLongitude"] is act["endLongitude"]):
                    exclusions.append(APIExcludeActivity("No points", activityId=act["activityId"]))
                    continue
                activity = UploadedActivity()

                try:
                    activity.TZ = pytz.timezone(act["activityTimeZone"]["key"])
                except pytz.exceptions.UnknownTimeZoneError:
                    activity.TZ = pytz.FixedOffset(float(act["activityTimeZone"]["offset"]) * 60)

                if len(act["activityName"]["value"].strip()) and act["activityName"]["value"] != "Untitled":
                    activity.Name = act["activityName"]["value"]
                # beginTimestamp is in UTC
                activity.StartTime = pytz.utc.localize(datetime.utcfromtimestamp(float(act["beginTimestamp"]["millis"])/1000))
                if "sumElapsedDuration" in act:
                    duration = timedelta(0, round(float(act["sumElapsedDuration"]["value"])))
                else:
                    duration = timedelta(minutes=float(act["sumDuration"]["minutesSeconds"].split(":")[0]), seconds=float(act["sumDuration"]["minutesSeconds"].split(":")[1]))
                activity.EndTime = activity.StartTime + duration
                activity.AdjustTZ()
                activity.Distance = float(act["sumDistance"]["value"]) * (1.60934 if act["sumDistance"]["uom"] == "mile" else 1)
                activity.Type = self._resolveActivityType(act["activityType"]["key"])

                activity.CalculateUID()
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["activityId"]}]
                activities.append(activity)
            if not exhaustive or int(res["search"]["totalPages"]) == page:
                break
            else:
                page += 1
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        #http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/#####?full=true
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        cookies = self._get_cookies(serviceRecord)
        res = requests.get("http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/" + str(activityID) + "?full=true", cookies=cookies)
        TCXIO.Parse(res.content, activity)
        return activity

    def UploadActivity(self, serviceRecord, activity):
        #/proxy/upload-service-1.1/json/upload/.tcx
        activity.EnsureTZ()
        tcx_file = TCXIO.Dump(activity)
        files = {"data": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".tcx", tcx_file)}
        cookies = self._get_cookies(serviceRecord)
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
