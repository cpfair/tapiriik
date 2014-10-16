import os
from datetime import datetime, timedelta
import dateutil.parser

import pytz
from dateutil.tz import tzutc
import requests
from django.core.urlresolvers import reverse

from tapiriik.settings import WEB_ROOT, NIKEPLUS_CLIENT_ID, NIKEPLUS_CLIENT_SECRET, NIKEPLUS_CLIENT_NAME
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.fit import FITIO
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

import logging
logger = logging.getLogger(__name__)

class NikePlusService(ServiceBase):
    ID = "nikeplus"
    DisplayName = "Nike+"
    DisplayAbbreviation = "N+"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    _activityMappings = {
        "RUN": ActivityType.Running,
        "JOGGING": ActivityType.Running,
        "WALK": ActivityType.Walking,
        "CYCLE": ActivityType.Cycling,
        "STATIONARY_BIKING": ActivityType.Cycling,
        "MOUNTAIN_BIKING": ActivityType.MountainBiking,
        "CROSS_COUNTRY": ActivityType.CrossCountrySkiing, # Well, I think?
        "ELLIPTICAL": ActivityType.Elliptical,
        "HIKING": ActivityType.Hiking,
        "ROCK_CLIMBING": ActivityType.Climbing,
        "ICE_CLIMBING": ActivityType.Climbing,
        "SNOWBOARDING": ActivityType.Snowboarding,
        "SKIING": ActivityType.DownhillSkiing,
        "ICE_SKATING": ActivityType.Skating,
        "OTHER": ActivityType.Other
    }

    SupportedActivities = []

    _sessionCache = SessionCache(lifetime=timedelta(minutes=45), freshen_on_get=False)

    _obligatoryHeaders = {
        "User-Agent": "NPConnect",
        "appId": NIKEPLUS_CLIENT_NAME
    }

    _obligatoryCookies = {
        "app": NIKEPLUS_CLIENT_NAME,
        "client_id": NIKEPLUS_CLIENT_ID,
        "client_secret": NIKEPLUS_CLIENT_SECRET
    }

    def _get_session(self, record=None, email=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        cached = self._sessionCache.Get(record.ExternalID if record else email)
        if cached and not skip_cache:
            return cached
        if record:
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        # This is the most pleasent login flow I've dealt with in a long time
        session = requests.Session()
        session.headers.update(self._obligatoryHeaders)
        session.cookies.update(self._obligatoryCookies)

        res = session.post("https://secure-nikeplus.nike.com/login/loginViaNike.do?mode=login", {"email": email, "password": password})

        if "access_token" not in res.cookies:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))


        # Was getting a super obscure error from the nether regions of requestse about duplicate cookies
        # So, store this in an easier-to-find location
        session.access_token = res.cookies["access_token"]

        self._sessionCache.Set(record.ExternalID if record else email, session)

        return session

    def _with_auth(self, session, params={}):
        # For whatever reason the access_token needs to be a GET parameter :(
        params.update({"access_token": session.access_token, "app": NIKEPLUS_CLIENT_NAME})
        return params

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(email=email, password=password)

        user_data = session.get("https://api.nike.com/nsl/user/get", params=self._with_auth(session, {"format": "json"}))
        user_id = int(user_data.json()["serviceResponse"]["body"]["User"]["id"])

        return (user_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def _durationToTimespan(self, duration):
        # Hours:Minutes:Seconds.Milliseconds
        duration = [float(x) for x in duration.split(":")]
        return timedelta(seconds=duration[2], minutes=duration[1], hours=duration[0])

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._get_session(serviceRecord)
        list_params = self._with_auth(session, {"count": 20, "offset": 1})

        activities = []
        exclusions = []

        while True:
            list_resp = session.get("https://api.nike.com/me/sport/activities", params=list_params)
            list_resp = list_resp.json()

            for act in list_resp["data"]:
                activity = UploadedActivity()
                activity.ServiceData = {"ID": act["activityId"]}

                if act["status"] != "COMPLETE":
                    exclusions.append(APIExcludeActivity("Not complete", activityId=act["activityId"], permanent=False, userException=UserException(UserExceptionType.LiveTracking)))
                    continue

                activity.StartTime = dateutil.parser.parse(act["startTime"]).replace(tzinfo=pytz.utc)
                activity.EndTime = activity.StartTime + self._durationToTimespan(act["metricSummary"]["duration"])

                activity.TZ = pytz.timezone(act["activityTimeZone"])

                if act["activityType"] in self._activityMappings:
                    activity.Type = self._activityMappings[act["activityType"]]

                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(act["metricSummary"]["distance"]))
                activity.Stats.Strides = ActivityStatistic(ActivityStatisticUnit.Strides, value=int(act["metricSummary"]["steps"]))
                activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(act["metricSummary"]["calories"]))

                # It's in the docs, but not getting returned :S
                if "isGpsActivity" in act:
                    activity.GPS = act["isGpsActivity"]

                activities.append(activity)

            if len(list_resp["data"]) == 0 or not exhaustive:
                break
            list_params["offset"] += list_params["count"]

        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        pass

    def UploadActivity(self, serviceRecord, activity):
        pass


    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
