from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException
#from tapiriik.auth.password_storage import PasswordStore

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
        if password is None:
            #  longing for C style overloads...
            password = email["ExtendedAuthorization"]["Password"]
            email = email["ExtendedAuthorization"]["Email"]
        params = {"login": "login", "login:loginUsernameField": email, "login:password": password, "login:signInButton": "Sign In", "javax.faces.ViewState": "j_id1"}
        preResp = requests.get("https://connect.garmin.com/signin")
        resp = requests.post("https://connect.garmin.com/signin", data=params, allow_redirects=False, cookies=preResp.cookies)
        if resp.status_code != 302:  # yep
            raise APIAuthorizationException("Invalid login")
        return preResp.cookies

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        cookies = self._get_cookies(email, password)
        username = requests.get("http://connect.garmin.com/user/username", cookies=cookies).json()["username"]
        return (username, {}, {"Email": email, "Password": password})


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
        while True:
            res = requests.get("http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities", data={"start": (page - 1) * pageSz, "limit": pageSz}, cookies=cookies)
            res = res.json()["results"]
            for act in res["activities"]:
                act = act["activity"]
                activity = UploadedActivity()
                try:
                    activity.TZ = pytz.timezone(act["activityTimeZone"]["key"])
                except pytz.exceptions.UnknownTimeZoneError:
                    activity.TZ = pytz.FixedOffset(float(act["activityTimeZone"]["offset"]) * 60)

                activity.StartTime = datetime.fromtimestamp(float(act["beginTimestamp"]["millis"])/1000)
                activity.EndTime = activity.StartTime + timedelta(0, round(float(act["sumElapsedDuration"]["value"])))
                activity.DefineTZ()
                activity.Distance = float(act["sumDistance"]["value"]) * (1.60934 if act["sumDistance"]["uom"] == "mile" else 1)
                activity.Type = self._resolveActivityType(act["activityType"]["key"])

                activity.CalculateUID()
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["activityId"]}]
                activities.append(activity)
            if not exhaustive or res["search"]["totalPages"] == page:
                break
            else:
                page += 1
        return activities

    def RevokeAuthorization(self, serviceRecord):
        #  nothing to do here...
        pass
