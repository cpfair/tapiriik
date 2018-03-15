from tapiriik.settings import WEB_ROOT, HTTP_SOURCE_ADDR
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, Location, Lap
from tapiriik.services.api import APIException, APIWarning, UserException, UserExceptionType
from tapiriik.services.sessioncache import SessionCache
from tapiriik.payments import ExternalPaymentProvider

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import logging
import time
import json
import tempfile
logger = logging.getLogger(__name__)

class MotivatoService(ServiceBase):
    ID = "motivato"
    DisplayName = "Motivato"
    DisplayAbbreviation = "MOT"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    _activityMappings={
        ActivityType.Running: 1,
        ActivityType.Cycling: 2,
        ActivityType.MountainBiking: 2,
        ActivityType.Walking: 7,
        ActivityType.Hiking: 7,
        ActivityType.DownhillSkiing: 5,
        ActivityType.CrossCountrySkiing: 5,
        ActivityType.Snowboarding: 5,
        ActivityType.Skating: 5,
        ActivityType.Swimming: 3,
        ActivityType.Wheelchair: 5,
        ActivityType.Rowing: 5,
        ActivityType.Elliptical: 5,
        ActivityType.Gym: 4,
        ActivityType.Climbing: 5,
        ActivityType.Other: 5,
    }

    _reverseActivityMappings={
        1: ActivityType.Running,
        2: ActivityType.Cycling,
        3: ActivityType.Swimming,
        4: ActivityType.Gym,
        5: ActivityType.Other,
        6: ActivityType.Other,
        7: ActivityType.Walking
    }

    SupportedActivities = list(_reverseActivityMappings.values())

    _sessionCache = SessionCache("motivato", lifetime=timedelta(minutes=30), freshen_on_get=True)
    _obligatory_headers = {
        "Referer": "https://sync.tapiriik.com"
    }

    _urlRoot = "http://motivato.pl"

    def __init__(self):
        rate_lock_path = tempfile.gettempdir() + "/m_rate.%s.lock" % HTTP_SOURCE_ADDR
        # Ensure the rate lock file exists (...the easy way)
        open(rate_lock_path, "a").close()
        self._rate_lock = open(rate_lock_path, "r+")

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def _getPaymentState(self, serviceRecord):
        # This method is also used by MotivatoExternalPaymentProvider to fetch user state
        session = self._get_session(record=serviceRecord)
        self._rate_limit()
        return session.get(self._urlRoot + "/api/tapiriikProfile").json()["isPremium"]

    def _applyPaymentState(self, serviceRecord):
        from tapiriik.auth import User
        state = self._getPaymentState(serviceRecord)
        ExternalPaymentProvider.FromID("motivato").ApplyPaymentState(User.GetByConnection(serviceRecord), state, serviceRecord.ExternalID, duration=None)

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(email=email, password=password)
        self._rate_limit()
        id = session.get(self._urlRoot + "/api/tapiriikProfile").json()["id"]
        if not len(id):
            raise APIException("Unable to retrieve username", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def UploadActivity(self, serviceRecord, activity):
        logger.debug("Motivato UploadActivity")
        session = self._get_session(record=serviceRecord)

        dic = dict(
            training_at=activity.StartTime.strftime("%Y-%m-%d"),
            distance=activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value,
            duration="",
            user_comment=activity.Notes,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            created_at=activity.StartTime.strftime("%Y-%m-%d %H:%M:%S"),
            discipline_id=self._activityMappings[activity.Type],
            source_id=8,
            metas=dict(
                distance=activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value,
                duration="",

                time_start=activity.StartTime.strftime("%H:%M:%S")
            ),
            track={}
        )

        if activity.Stats.TimerTime.Value is not None:
            secs = activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        elif activity.Stats.MovingTime.Value is not None:
            secs = activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        else:
            secs = (activity.EndTime - activity.StartTime).total_seconds()


        dic["metas"]["duration"] = str(timedelta(seconds=secs))
        dic["duration"] = str(timedelta(seconds=secs))

        pace=str(timedelta(seconds=secs/activity.Stats.Distance.Value))
        meta_hr_avg=activity.Stats.HR.Average
        meta_hr_max=activity.Stats.HR.Max

        if pace:
            dic["metas"]["pace"] = pace

        if meta_hr_avg:
            dic["metas"]["meta_hr_avg"] = meta_hr_avg

        if meta_hr_max:
            dic["metas"]["meta_hr_max"] = meta_hr_max

        if len(activity.Laps) > 0:
            dic["track"] = dict(
                name=activity.Name,
                mtime=secs,
                points=[]
            )

            for tk in activity.Laps:
                for wpt in tk.Waypoints:
                    pt = dict(
                        lat=wpt.Location.Latitude,
                        lon=wpt.Location.Longitude,
                        ele=wpt.Location.Altitude,
                        bpm=wpt.HR,
                        moment=wpt.Timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    )

                    if wpt.Speed and wpt.Speed != None and wpt.Speed != 0:
                        pt["pace"]=(1000.0/wpt.Speed)

                    dic["track"]["points"].append(pt)

        toSend = json.dumps(dic)

        try:
            res = session.post(self._urlRoot + "/api/workout", data=toSend)
        except APIWarning as e:
            raise APIException(str(e))

        if res.status_code != 201:
            raise APIException("Activity didn't upload: %s, %s" % (res.status_code, res.text))

        try:
            retJson = res.json()
        except ValueError:
            raise APIException("Activity upload parse error for %s, %s" % (res.status_code, res.text))

        return retJson["id"]

    def _parseDate(self, date):
        return datetime.strptime(date, "%Y-%m-%d")

    def _parseDateTime(self, date):
        try:
            return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.strptime(date, "%Y-%m-%d %H:%M")

    def _durationToSeconds(self, dur):
        # in order to fight broken metas
        parts = dur.split(":")
        return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        logger.debug("Checking motivato premium state")
        self._applyPaymentState(serviceRecord)

        logger.debug("Motivato DownloadActivityList")
        session = self._get_session(record=serviceRecord)
        activities = []
        exclusions = []

        self._rate_limit()

        retried_auth = False
        #headers = {'X-App-With-Tracks': "true"}
        headers = {}
        res = session.post(self._urlRoot + "/api/workouts/sync", headers=headers)

        if res.status_code == 403 and not retried_auth:
            retried_auth = True
            session = self._get_session(serviceRecord, skip_cache=True)

        try:
            respList = res.json();
        except ValueError:
            res_txt = res.text # So it can capture in the log message
            raise APIException("Parse failure in Motivato list resp: %s" % res.status_code)

        for actInfo in respList:
            if "duration" in actInfo:
                duration = self._durationToSeconds(actInfo["duration"])
            else:
                continue

            activity = UploadedActivity()
            if "time_start" in actInfo["metas"]:
                startTimeStr = actInfo["training_at"] + " " + actInfo["metas"]["time_start"]
            else:
                startTimeStr = actInfo["training_at"] + " 00:00:00"

            activity.StartTime = self._parseDateTime(startTimeStr)
            activity.EndTime = self._parseDateTime(startTimeStr) + timedelta(seconds=duration)
            activity.Type = self._reverseActivityMappings[actInfo["discipline_id"]]
            activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=duration)
            if "distance" in actInfo:
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(actInfo["distance"]))
            #activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerSecond, value=1.0/float(actInfo["metas"]["pace"]))

            activity.ServiceData={"WorkoutID": int(actInfo["id"])}

            activity.CalculateUID()
            logger.debug("Generated UID %s" % activity.UID)
            activities.append(activity)


        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        workoutID = activity.ServiceData["WorkoutID"]
        logger.debug("DownloadActivity for %s" % workoutID);

        session = self._get_session(record=serviceRecord)

        resp = session.get(self._urlRoot + "/api/workout/%d" % workoutID)

        try:
            res = resp.json()
        except ValueError:
            raise APIException("Parse failure in Motivato activity (%d) download: %s" % (workoutID, res.text))

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]
        activity.GPS = False
        if "track" in res and "points" in res["track"]:
            for pt in res["track"]["points"]:
                wp = Waypoint()
                if "moment" not in pt:
                    continue
                wp.Timestamp = self._parseDateTime(pt["moment"])

                if ("lat" in pt and "lon" in pt) or "ele" in pt:
                    wp.Location = Location()
                    if "lat" in pt and "lon" in pt:
                        wp.Location.Latitude = pt["lat"]
                        wp.Location.Longitude = pt["lon"]
                        activity.GPS = True
                    if "ele" in pt:
                        wp.Location.Altitude = float(pt["ele"])

                if "bpm" in pt:
                    wp.HR = pt["bpm"]

                lap.Waypoints.append(wp)

        activity.Stationary = len(lap.Waypoints) == 0

        return activity

    def _get_session(self, record=None, email=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        cached = self._sessionCache.Get(record.ExternalID if record else email)
        if cached and not skip_cache:
            return cached
        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()
        self._rate_limit()
        mPreResp = session.get(self._urlRoot + "/api/tapiriikProfile", allow_redirects=False)
        # New site gets this redirect, old one does not
        if mPreResp.status_code == 403:
            data = {
                "_username": email,
                "_password": password,
                "_remember_me": "true",
            }
            preResp = session.post(self._urlRoot + "/api/login", data=data)

            if preResp.status_code != 200:
                raise APIException("Login error %s %s" % (preResp.status_code, preResp.text))

            try:
                preResp = preResp.json()
            except ValueError:
                raise APIException("Parse error %s %s" % (preResp.status_code, preResp.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            if "success" not in preResp and "error" not in preResp:
                raise APIException("Login error", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            success = True
            error = ""

            if "success" in preResp:
                success = ["success"]

            if "error" in preResp:
                error = preResp["error"]

            if not success:
                logger.debug("Login error %s" % (error))
                raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            # Double check


            self._rate_limit()
            mRedeemResp1 = session.get(self._urlRoot + "/api/tapiriikProfile", allow_redirects=False)
            if mRedeemResp1.status_code != 200:
                raise APIException("Motivato redeem error %s %s" % (mRedeemResp1.status_code, mRedeemResp1.text))

        else:
            logger.debug("code %s" % mPreResp.status_code)
            raise APIException("Unknown Motivato prestart response %s %s" % (mPreResp.status_code, mPreResp.text))

        self._sessionCache.Set(record.ExternalID if record else email, session)

        session.headers.update(self._obligatory_headers)

        return session

    def _rate_limit(self):
        import fcntl, time
        min_period = 1
        print("Waiting for lock")
        fcntl.flock(self._rate_lock,fcntl.LOCK_EX)
        try:
            print("Have lock")
            self._rate_lock.seek(0)
            last_req_start = self._rate_lock.read()
            if not last_req_start:
                last_req_start = 0
            else:
                last_req_start = float(last_req_start)

            wait_time = max(0, min_period - (time.time() - last_req_start))
            time.sleep(wait_time)

            self._rate_lock.seek(0)
            self._rate_lock.write(str(time.time()))
            self._rate_lock.flush()

            print("Rate limited for %f" % wait_time)
        finally:
            fcntl.flock(self._rate_lock,fcntl.LOCK_UN)

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass
