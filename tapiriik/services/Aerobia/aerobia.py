# Synchronization module for aerobia.ru
# (c) 2018 Anton Ashmarin, aashmarin@gmail.com
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO

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
    
    # common -> aerobia (garmin tcx sport names)
    # todo may better to include this into tcxio logic instead
    _activityMappings = {
        ActivityType.Running : "Running",
        ActivityType.Cycling : "Biking",
        ActivityType.MountainBiking : "Mountain biking",
        ActivityType.Walking : "Walking",
        ActivityType.Hiking : "Hiking",
        ActivityType.DownhillSkiing : "Skiing downhill",
        ActivityType.CrossCountrySkiing : "Cross country skiing",
        ActivityType.Skating : "Skating",
        ActivityType.Swimming : "Swimming",
        ActivityType.Rowing : "Rowing",
        ActivityType.Elliptical : "Ellips",
        ActivityType.Gym : "Gym",
        ActivityType.Climbing : "Rock climbing",
        ActivityType.StrengthTraining : "Ofp",
        ActivityType.Other : "Sport"
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

    _urlRoot = "http://aerobia.ru/"
    _apiRoot = "http://aerobia.ru/api/"
    _loginUrlRoot = _apiRoot + "sign_in"
    _workoutsUrl = _apiRoot + "workouts"
    _workoutUrl = _apiRoot + "workouts/{id}.json"
    _uploadsUrl = _apiRoot + "uploads.json"

    def _patch_user_agent(self):
        from tapiriik.requests_lib import patch_requests_user_agent
        # Without user-agent patch aerobia requests doesn't work
        patch_requests_user_agent('Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko')

    def _get_auth_data(self, record=None, username=None, password=None):
        from tapiriik.auth.credential_storage import CredentialStore

        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            username = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        self._patch_user_agent()
        request_parameters = {"user[email]": username, "user[password]": password}
        res = requests.post(self._loginUrlRoot, data=request_parameters)

        if res.status_code != 200:
            raise APIException("Login exception {} - {}".format(res.status_code, res.text), user_exception=UserException(UserExceptionType.Authorization))

        res_xml = etree.fromstring(res.text.encode('utf-8'))

        info = res_xml.find("info")
        if info.get("status") != "ok":
            raise APIException(info.get("description"), user_exception=UserException(UserExceptionType.Authorization))

        user_id = int(res_xml.find("user/id").get("value"))
        user_token = res_xml.find("user/authentication_token").get("value")

        return user_id, user_token

    def _refresh_token(self, record):
        logger.info("refreshing auth token")
        user_id, user_token = self._get_auth_data(record=record)
        record.Authorization.update({"OAuthToken": user_token})

    def _with_auth(self, record, params={}):
        params.update({"authentication_token": record.Authorization["OAuthToken"]})
        return params

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        user_id, user_token = self._get_auth_data(username=username, password=password)

        secret = {
            "Email": CredentialStore.Encrypt(username), 
            "Password": CredentialStore.Encrypt(password)
            }
        authorizationData = {"OAuthToken": user_token}
        
        return (user_id, authorizationData, secret)

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        self._patch_user_agent()

        activities = []
        exclusions = []
        dairy_xml = None

        # use first query responce to detect pagination options as well
        try:
            dairy_xml = self._get_dairy_xml(serviceRecord)
        except APIException:
            # try to refresh token first
            self._refresh_token(serviceRecord)
            dairy_xml = self._get_dairy_xml(serviceRecord)

        pagination = dairy_xml.find("pagination")
        # New accounts have no data pages initially
        total_pages_str = pagination.get("total_pages")
        total_pages = int(total_pages_str) if total_pages_str else 1
        
        for page in range(2, total_pages + 2):
            for workout_info in dairy_xml.findall("workouts/r"):
                activity = self._create_activity(workout_info)
                activities.append(activity)
            
            if not exhaustive or page > total_pages:
                break

            dairy_xml = self._get_dairy_xml(serviceRecord, {"page": page})

        return activities, exclusions

    def _get_dairy_xml(self, serviceRecord, params={}):
        dairy_data = requests.get(self._workoutsUrl, params=self._with_auth(serviceRecord, params))
        dairy_xml = etree.fromstring(dairy_data.text.encode('utf-8'))

        info = dairy_xml.find("info")
        if info.get("status") != "ok":
            raise APIException(info.get("description"), user_exception=UserException(UserExceptionType.DownloadError))

        return dairy_xml

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
        self._patch_user_agent()
        activity_id = activity.ServiceData["ActivityID"]

        tcx_data = requests.get("{}export/workouts/{}/tcx".format(self._urlRoot, activity_id), data=self._with_auth(serviceRecord))
        activity_ex = TCXIO.Parse(tcx_data.text.encode('utf-8'), activity)
        # Obtain more information about activity
        res = requests.get(self._workoutUrl.format(id=activity_id), data=self._with_auth(serviceRecord))
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
        # todo use correct mapping to upload activity correctly
        tcx_data = TCXIO.Dump(activity, self._activityMappings[activity.Type])
        #tcx_data = TCXIO.Dump(activity)
        data = {"name": activity.Name,
                "description": activity.Notes}
        files = {"file": ("tap-sync-{}-{}.tcx".format(os.getpid(), activity.UID), tcx_data)}
        res = requests.post(self._uploadsUrl, data=self._with_auth(serviceRecord, data), files=files)
        res_obj = res.json()

        if "error" in res_obj:
            raise APIException(res_obj["error"], user_exception=UserException(UserExceptionType.UploadError))
        
        # return just uploaded activity id
        return res_obj["workouts"][0]["id"]

    def UserUploadedActivityURL(self, uploadId):
        raise NotImplementedError
        # TODO need to include user id
        #return self.UserActivityURL.format(userId, uploadId)

    def DeleteActivity(self, serviceRecord, uploadId):
        self._patch_user_agent()

        delete_parameters = {"_method" : "delete"}
        requests.post("{}workouts/{}".format(self._urlRoot, uploadId), data=self._with_auth(serviceRecord, delete_parameters))
        pass

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass