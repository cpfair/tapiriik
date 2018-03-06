# Synchronisation module for aerobia.ru
# (c) 2018 Anton Ashmarin, aashmarin@gmail.com
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

from lxml import etree
from bs4 import BeautifulSoup

import requests
import logging
import re
import os
import pytz

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AerobiaService(ServiceBase):
    ID = "aerobia"
    DisplayName = "Aerobia"
    DisplayAbbreviation = "ARB"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    UserProfileURL = "http://www.aerobia.ru/users/{0}"
    UserActivityURL = "http://www.aerobia.ru/users/{0}/workouts/{1}"
    
    # common -> aerobia
    _activityMappings = {
        ActivityType.Running : 2,
        ActivityType.Cycling : 1,
        ActivityType.MountainBiking : 56,
        ActivityType.Walking : 19,
        ActivityType.Hiking : 43,
        ActivityType.DownhillSkiing : 9,
        ActivityType.CrossCountrySkiing : 3,
        ActivityType.Skating : 46,
        ActivityType.Swimming : 21,
        ActivityType.Rowing : 13,
        ActivityType.Elliptical : 74,
        ActivityType.Gym : 54,
        ActivityType.Climbing : 63,
        ActivityType.StrengthTraining : 72,
        ActivityType.Other : 68
    }

    # aerobia -> common
    _reverseActivityMappings = {
        1 : ActivityType.Cycling,
        2 : ActivityType.Running,
        56 : ActivityType.MountainBiking,
        19 : ActivityType.Walking,
        43 : ActivityType.Hiking,
        9 : ActivityType.DownhillSkiing,
        3 : ActivityType.CrossCountrySkiing,
        46 : ActivityType.Skating,
        21 : ActivityType.Swimming,
        13 : ActivityType.Rowing,
        74 : ActivityType.Elliptical,
        54 : ActivityType.Gym,
        63 : ActivityType.Climbing,
        72 : ActivityType.StrengthTraining,

        6 : ActivityType.Cycling, #cycling transport
        22 : ActivityType.Cycling, #indoor cycling
        73 : ActivityType.Gym, #stretching
        76 : ActivityType.Gym, #trx
        83 : ActivityType.CrossCountrySkiing, #classic skiing
        65 : ActivityType.Other, #triathlon
        51 : ActivityType.Other, #beach volleyball
        53 : ActivityType.Other, #basketball
        55 : ActivityType.Other, #roller sport
        77 : ActivityType.Running, #tredmill
        66 : ActivityType.Other, #roller skiing
        7 : ActivityType.Other, #rollers
        58 : ActivityType.Other, #nordic walking
        10 : ActivityType.Other, #snowboarding
        16 : ActivityType.Other, #walking sport
        18 : ActivityType.Other, #orienting
        38 : ActivityType.Other, #
        61 : ActivityType.Other, #
        79 : ActivityType.Other, #
        23 : ActivityType.Other, #
        26 : ActivityType.Other, #
        84 : ActivityType.Other, #
        24 : ActivityType.Other, #
        52 : ActivityType.Other, #
        50 : ActivityType.Other, #
        49 : ActivityType.Other, #
        48 : ActivityType.Other, #
        4 : ActivityType.Other, #
        36 : ActivityType.Other, #
        85 : ActivityType.Other, #
        69 : ActivityType.Other, #
        47 : ActivityType.Other, #
        45 : ActivityType.Other, #
        80 : ActivityType.Other, #
        62 : ActivityType.Other, #
        71 : ActivityType.Other, #
        64 : ActivityType.Other, #
        78 : ActivityType.Other, #
        44 : ActivityType.Other, #
        70 : ActivityType.Other, #
        35 : ActivityType.Other, #
        20 : ActivityType.Other, #
        33 : ActivityType.Other, #
        60 : ActivityType.Other, #
        67 : ActivityType.Other, #
        15 : ActivityType.Other, #
        42 : ActivityType.Other, #
        41 : ActivityType.Other, #
        75 : ActivityType.Other, #
        29 : ActivityType.Other, #
        40 : ActivityType.Other, #
        37 : ActivityType.Other, #
        81 : ActivityType.Other, #
        31 : ActivityType.Other, #
        59 : ActivityType.Other, #
        39 : ActivityType.Other, #
        34 : ActivityType.Other, #
        82 : ActivityType.Other, #

        68 : ActivityType.Other
    }

    SupportedActivities = list(_activityMappings.keys())

    SupportsHR = SupportsCadence = True

    SupportsActivityDeletion = True

    _sessionCache = SessionCache("aerobia_session", lifetime=timedelta(minutes=120), freshen_on_get=True)
    _tokenCache = SessionCache("aerobia_token", lifetime=timedelta(days=1), freshen_on_get=True)

    _urlRoot = "http://aerobia.ru/"
    _apiRoot = "http://aerobia.ru/api/"
    _loginUrlRoot = _apiRoot + "sign_in"
    _workoutsUrl = _apiRoot + "workouts"
    _workoutUrl = _apiRoot + "workouts/{id}.json"
    _uploadsUrl = _apiRoot + "uploads.json"

    def _get_session(self, record=None, username=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        from tapiriik.requests_lib import patch_requests_user_agent

        # Without user-agent patch aerobia requests doesn't work
        patch_requests_user_agent('Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko')

        cached = self._sessionCache.Get(record.ExternalID if record else username)
        if cached and not skip_cache:
            logger.debug("Using cached credential")
            return cached
        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            username = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()

        request_parameters = {"user[email]": username, "user[password]": password}
        res = session.post(self._loginUrlRoot, data=request_parameters)

        if res.status_code != 200:
            raise APIException("Login exception {} - {}".format(res.status_code, res.text), user_exception=UserException(UserExceptionType.Authorization))

        res_xml = etree.fromstring(res.text.encode('utf-8'))

        info = res_xml.find("info")
        if info.get("status") != "ok":
            raise APIException(info.get("description"), user_exception=UserException(UserExceptionType.Authorization))

        session.user_id = res_xml.find("user/id").get("value")
        # store response token
        token = res_xml.find("user/authentication_token").get("value")
        self._tokenCache.Set(record.ExternalID if record else username, token)

        self._sessionCache.Set(record.ExternalID if record else username, session)

        return session

    def _get_user_token(self, record):
        userToken = None
        if record and record.ExternalID:
            userToken = self._tokenCache.Get(record.ExternalID)
        return userToken

    def _with_auth(self, record, params={}):
        params.update({"authentication_token": self._get_user_token(record)})
        return params

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(username=username, password=password, skip_cache=True)

        secret = {
            "Email": CredentialStore.Encrypt(username), 
            "Password": CredentialStore.Encrypt(password)
            }
        return (session.user_id, {}, secret)

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._get_session(serviceRecord)

        activities = []
        exclusions = []

        # use first query responce to detect pagination options as well
        dairy_data = session.get(self._workoutsUrl, params=self._with_auth(serviceRecord))
        dairy_xml = etree.fromstring(dairy_data.text.encode('utf-8'))

        info = dairy_xml.find("info")
        if info.get("status") != "ok":
            raise APIException(info.get("description"), user_exception=UserException(UserExceptionType.DownloadError))
        
        pagination = dairy_xml.find("pagination")
        #workouts_per_page = int(pagination.get("per_page"))
        # New accounts have no data pages initially
        total_pages_str = pagination.get("total_pages")
        total_pages = int(total_pages_str) if total_pages_str else 1
        
        for page in range(2, total_pages + 2):
            for workout_info in dairy_xml.findall("workouts/r"):
                activity = self._create_activity(workout_info)
                activities.append(activity)
            
            if not exhaustive or page > total_pages:
                break

            page_param = {"page": page}
            dairy_data = session.get(self._workoutsUrl, params=self._with_auth(serviceRecord, page_param))
            dairy_xml = etree.fromstring(dairy_data.text.encode('utf-8'))

        return activities, exclusions

    def _create_activity(self, data):
        activity = UploadedActivity()
        activity.Name = data.get("name")
        activity.StartTime = pytz.utc.localize(datetime.strptime(data.get("start_at"), "%Y-%m-%dT%H:%M:%SZ"))
        activity.EndTime = activity.StartTime + timedelta(0, float(data.get("duration")))
        sport_id = data.get("sport_id")
        activity.Type = self._reverseActivityMappings[int(sport_id)] if sport_id else ActivityType.Other

        distance = data.get("distance")
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(distance) if distance else None)
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(data.get("total_time_in_seconds")))
        avg_speed = data.get("average_speed")
        max_speed = data.get("max_speed")
        activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=float(avg_speed) if avg_speed else None, max=float(max_speed) if max_speed else None)
        avg_hr = data.get("average_heart_rate")
        max_hr = data.get("maximum_heart_rate")
        activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(avg_hr) if avg_hr else None, max=float(max_hr) if max_hr else None))
        calories = data.get("calories")
        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=int(calories) if calories else None)

        activity.ServiceData = {"ActivityID": data.get("id")}

        logger.debug("\tActivity s/t {}: {}".format(activity.StartTime, activity.Type))
        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        session = self._get_session(serviceRecord)
        activity_id = activity.ServiceData["ActivityID"]

        tcx_data = session.get("{}export/workouts/{}/tcx".format(self._urlRoot, activity_id), data=self._with_auth(serviceRecord))
        activity_ex = TCXIO.Parse(tcx_data.text.encode('utf-8'), activity)
        # Obtain more information about activity
        res = session.get(self._workoutUrl.format(id=activity_id), data=self._with_auth(serviceRecord))
        activity_data = res.json()
        activity_ex.Name = activity_data["name"]
        # Notes comes as html. Hardly any other service will support this so needs to extract text data
        if "body" in activity_data["post"]:
            post_html = activity_data["post"]["body"]
            soup = BeautifulSoup(post_html)
            # Notes also contains styles, get rid of them
            for style in soup("style"):
                style.decompose()
            activity_ex.Notes = soup.getText()

        return activity_ex

    def UploadActivity(self, serviceRecord, activity):
        session = self._get_session(serviceRecord)
        # todo use correct mapping to upload activity correctly
        #tcx_data = TCXIO.Dump(activity, self._activityMappings[activity.Type])
        tcx_data = TCXIO.Dump(activity)
        data = {"name": activity.Name,
                "description": activity.Notes}
        files = {"file": ("tap-sync-{}-{}.tcx".format(os.getpid(), activity.UID), tcx_data)}
        res = session.post(self._uploadsUrl, data=self._with_auth(serviceRecord, data), files=files)
        res_obj = res.json()

        if "error" in res_obj:
            raise APIException(res_obj["error"], user_exception=UserException(UserExceptionType.UploadError))
        
        # return just uploaded activity id
        return res_obj["workouts"][0]["id"]

    def DeleteActivity(self, serviceRecord, uploadId):
        session = self._get_session(serviceRecord)

        delete_parameters = {"_method" : "delete"}
        session.post("{}workouts/{}".format(self._urlRoot, uploadId), data=self._with_auth(serviceRecord, delete_parameters))

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass
