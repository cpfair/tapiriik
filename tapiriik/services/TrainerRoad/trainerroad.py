import os
import pytz
import requests
import dateutil.parser
from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from lxml import etree

from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.tcx import TCXIO

import logging
logger = logging.getLogger(__name__)

class TrainerRoadService(ServiceBase):
    ID = "trainerroad"
    DisplayName = "TrainerRoad"
    DisplayAbbreviation = "TR"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesActivities = False

    SupportedActivities = [ActivityType.Cycling]

    def _get_session(self, username=None, password=None, record=None, cookieAuth=False):
        from tapiriik.auth.credential_storage import CredentialStore
        if record:
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            username = CredentialStore.Decrypt(record.ExtendedAuthorization["Username"])

        session = requests.Session()

        if cookieAuth:
            login_res = session.post(
                "https://www.trainerroad.com/login",
                cookies={"__RequestVerificationToken": "whee"},
                data={"Username": username, "Password": password, "__RequestVerificationToken": "whee"},
                allow_redirects=False
            )
            if login_res.status_code != 302:
                raise APIException("Invalid login %s - %s" % (login_res.status_code, login_res.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        else:
            session.auth = (username, password)

        return session

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(username, password)
        session.headers.update({"Accept": "application/json"})
        user_resp = session.get("https://api.trainerroad.com/api/members")

        if user_resp.status_code != 200:
            if user_resp.status_code == 401:
                raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Login error")

        member_id = int(user_resp.json()["MemberId"])

        return (member_id, {}, {"Username": CredentialStore.Encrypt(username), "Password": CredentialStore.Encrypt(password)})

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        activities = []
        session = self._get_session(record=serviceRecord)
        session.headers.update({"Accept": "application/json"})
        workouts_resp = session.get("https://api.trainerroad.com/api/careerworkouts")

        if workouts_resp.status_code != 200:
            if workouts_resp.status_code == 401:
                raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Workout listing error")

        cached_record = cachedb.trainerroad_meta.find_one({"ExternalID": serviceRecord.ExternalID})
        if not cached_record:
            cached_workout_meta = {}
        else:
            cached_workout_meta = cached_record["Workouts"]

        workouts = workouts_resp.json()
        for workout in workouts:
            # Un/f their API doesn't provide the start/end times in the list response
            # So we need to pull the extra data, if it's not already cached
            workout_id = str(workout["Id"]) # Mongo doesn't do non-string keys
            if workout_id not in cached_workout_meta:
                meta_resp = session.get("https://api.trainerroad.com/api/careerworkouts?guid=%s" % workout["Guid"])
                # We don't need everything
                full_meta = meta_resp.json()
                meta = {key: full_meta[key] for key in ["WorkoutDate", "WorkoutName", "WorkoutNotes", "TotalMinutes", "TotalKM", "AvgWatts", "Kj"]}
                cached_workout_meta[workout_id] = meta
            else:
                meta = cached_workout_meta[workout_id]

            activity = UploadedActivity()
            activity.ServiceData = {"ID": int(workout_id)}
            activity.Name = meta["WorkoutName"]
            activity.Notes = meta["WorkoutNotes"]
            activity.Type = ActivityType.Cycling

            # Everything's in UTC
            activity.StartTime = dateutil.parser.parse(meta["WorkoutDate"]).replace(tzinfo=pytz.utc)
            activity.EndTime = activity.StartTime + timedelta(minutes=meta["TotalMinutes"])

            activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=meta["TotalKM"])
            activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=meta["AvgWatts"])
            activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilojoules, value=meta["Kj"])

            activity.Stationary = False
            activity.GPS = False
            activity.CalculateUID()

            activities.append(activity)

        cachedb.trainerroad_meta.update({"ExternalID": serviceRecord.ExternalID}, {"ExternalID": serviceRecord.ExternalID, "Workouts": cached_workout_meta}, upsert=True)

        return activities, []

    def DownloadActivity(self, serviceRecord, activity):
        workout_id = activity.ServiceData["ID"]

        session = self._get_session(record=serviceRecord)

        res = session.get("http://www.trainerroad.com/cycling/rides/download/%d" % workout_id)

        if res.status_code == 500:
            # Account is private (or their site is borked), log in the blegh way
            session = self._get_session(record=serviceRecord, cookieAuth=True)
            res = session.get("http://www.trainerroad.com/cycling/rides/download/%d" % workout_id)
            activity.Private = True

        try:
            TCXIO.Parse(res.content, activity)
        except ValueError as e:
            raise APIExcludeActivity("TCX parse error " + str(e), user_exception=UserException(UserExceptionType.Corrupt))

        return activity

    def UploadActivity(self, serviceRecord, activity):
        # Nothing to see here.
        pass

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        cachedb.trainerroad_meta.remove({"ExternalID": serviceRecord.ExternalID})
