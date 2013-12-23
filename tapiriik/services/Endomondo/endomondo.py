from tapiriik.settings import WEB_ROOT, ENDOMONDO_CLIENT_KEY, ENDOMONDO_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.sessioncache import SessionCache
from tapiriik.services.fit import FITIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
from requests_oauthlib import OAuth1Session
import pytz
import re
import zlib
import os
import logging
import pickle
import calendar

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://www.endomondo.com/profile/{0}"
    UserActivityURL = "http://www.endomondo.com/workouts/{1}/{0}"

    PartialSyncRequiresTrigger = True
    AuthenticationNoFrame = True

    _activityMappings = {
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,  # the order of these matters since it picks the first match for uploads
        1:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        9:  ActivityType.Rowing,  # canoeing
        18: ActivityType.Walking,
        14: ActivityType.Walking,  # fitness walking
        16: ActivityType.Hiking,
        17: ActivityType.Hiking,  # orienteering
        20: ActivityType.Swimming,
        40: ActivityType.Swimming,  # scuba diving
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    _reverseActivityMappings = {  # so that ambiguous events get mapped back to reasonable types
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        18: ActivityType.Walking,
        16: ActivityType.Hiking,
        20: ActivityType.Swimming,
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways

    _oauth_token_secrets = {}

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "endomondo"})

    def _oauthSession(self, connection=None, **params):
        if connection:
            params["resource_owner_key"] = connection.Authorization["Token"]
            params["resource_owner_secret"] = connection.Authorization["Secret"]
        return OAuth1Session(ENDOMONDO_CLIENT_KEY, client_secret=ENDOMONDO_CLIENT_SECRET, **params)

    def GenerateUserAuthorizationURL(self, level=None):
        oauthSession = self._oauthSession(callback_uri=WEB_ROOT + reverse("oauth_return", kwargs={"service": "endomondo"}))
        tokens = oauthSession.fetch_request_token("https://api.endomondo.com/oauth/request_token")
        self._oauth_token_secrets[tokens["oauth_token"]] = tokens["oauth_token_secret"]
        return oauthSession.authorization_url("https://www.endomondo.com/oauth/authorize")

    def RetrieveAuthorizationToken(self, req, level):
        oauthSession = self._oauthSession(resource_owner_secret=self._oauth_token_secrets[req.GET["oauth_token"]])
        oauthSession.parse_authorization_response(req.get_full_path())
        tokens = oauthSession.fetch_access_token("https://api.endomondo.com/oauth/access_token")
        userInfo = oauthSession.get("https://api.endomondo.com/api/1/user")
        userInfo = userInfo.json()
        return (userInfo["id"], {"Token": tokens["oauth_token"], "Secret": tokens["oauth_token_secret"]})

    def RevokeAuthorization(self, serviceRecord):
        pass

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        activities = []
        exclusions = []

        return activities, exclusions

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).put("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        assert resp.status_code in [200, 201] # Created, or already existed

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).delete("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        assert resp.status_code in [204, 500] # Docs say otherwise, but no-subscription-found is 500

    def DownloadActivity(self, serviceRecord, activity):
        return activity

    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass
