from tapiriik.settings import WEB_ROOT, TRAININGPEAKS_CLIENT_ID, TRAININGPEAKS_CLIENT_SECRET, TRAININGPEAKS_CLIENT_SCOPE, TRAININGPEAKS_OAUTH_BASE_URL, TRAININGPEAKS_API_BASE_URL
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.pwx import PWXIO
from tapiriik.services.sessioncache import SessionCache

from datetime import datetime, timedelta
from urllib.parse import urlencode
import dateutil.parser
import requests
import logging
from io import BytesIO
import gzip
import base64
import json
import time

logger = logging.getLogger(__name__)

class TrainingPeaksService(ServiceBase):
    ID = "trainingpeaks"
    DisplayName = "TrainingPeaks"
    DisplayAbbreviation = "TP"
    AuthenticationType = ServiceAuthenticationType.OAuth
    RequiresExtendedAuthorizationDetails = False
    ReceivesStationaryActivities = False
    SuppliesActivities = False
    AuthenticationNoFrame = True
    SupportsExhaustiveListing = False

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    # Not-so-coincidentally, similar to PWX.
    _workoutTypeMappings = {
        "bike": ActivityType.Cycling,
        "run": ActivityType.Running,
        "walk": ActivityType.Walking,
        "swim": ActivityType.Swimming,
        "mtb": ActivityType.MountainBiking,
        "xc-Ski": ActivityType.CrossCountrySkiing,
        "rowing": ActivityType.Rowing,
        "x-train": ActivityType.Other,
        "strength": ActivityType.Other,
        "other": ActivityType.Other,
    }
    SupportedActivities = ActivityType.List() # All.

    _redirect_url = "https://tapiriik.com/auth/return/trainingpeaks"
    _tokenCache = SessionCache("trainingpeaks", lifetime=timedelta(minutes=30), freshen_on_get=False)

    def WebInit(self):
        self.UserAuthorizationURL = TRAININGPEAKS_OAUTH_BASE_URL + "/oauth/authorize?" + urlencode({
            "client_id": TRAININGPEAKS_CLIENT_ID,
            "response_type": "code",
            "redirect_uri": self._redirect_url,
            "scope": TRAININGPEAKS_CLIENT_SCOPE
        })

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {
            "client_id": TRAININGPEAKS_CLIENT_ID,
            "client_secret": TRAININGPEAKS_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self._redirect_url
        }

        req_url = TRAININGPEAKS_OAUTH_BASE_URL + "/oauth/token"
        response = requests.post(req_url, data=params)
        if response.status_code != 200:
            raise APIException("Invalid code")
        auth_data = response.json()

        profile_data = requests.get(TRAININGPEAKS_API_BASE_URL + "/v1/athlete/profile",
                                    headers={"Authorization": "Bearer %s" % auth_data["access_token"]}).json()
        if type(profile_data) is list and any("is not a valid athlete" in x for x in profile_data):
            raise APIException("TP user is coach account", block=True, user_exception=UserException(UserExceptionType.NonAthleteAccount, intervention_required=True))
        return (profile_data["Id"], {"RefreshToken": auth_data["refresh_token"]})

    def _apiHeaders(self, serviceRecord):
        # The old API was username/password, and the new API provides no means to automatically upgrade these credentials.
        if not serviceRecord.Authorization or "RefreshToken" not in serviceRecord.Authorization:
            raise APIException("TP user lacks OAuth credentials", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        token = self._tokenCache.Get(serviceRecord.ExternalID)
        if not token:
            # Use refresh token to get access token
            # Hardcoded return URI to get around the lack of URL reversing without loading up all the Django stuff
            params = {
                "client_id": TRAININGPEAKS_CLIENT_ID,
                "client_secret": TRAININGPEAKS_CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": serviceRecord.Authorization["RefreshToken"],
                # "redirect_uri": self._redirect_url
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(TRAININGPEAKS_OAUTH_BASE_URL + "/oauth/token", data=urlencode(params), headers=headers)
            if response.status_code != 200:
                if response.status_code >= 400 and response.status_code < 500:
                    raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text))
            token = response.json()["access_token"]
            self._tokenCache.Set(serviceRecord.ExternalID, token)

        return {"Authorization": "Bearer %s" % token}

    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def DownloadActivityList(self, svcRecord, exhaustive_start_time=None):
        activities = []
        exclusions = []

        headers = self._apiHeaders(svcRecord)

        limitDateFormat = "%Y-%m-%d"

        if exhaustive_start_time:
            totalListEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            totalListStart = exhaustive_start_time - timedelta(days=1.5)
        else:
            totalListEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            totalListStart = totalListEnd - timedelta(days=20) # Doesn't really matter

        listStep = timedelta(days=45)
        listEnd = totalListEnd
        listStart = max(totalListStart, totalListEnd - listStep)

        while True:
            logger.debug("Requesting %s to %s" % (listStart, listEnd))
            resp = requests.get(
                TRAININGPEAKS_API_BASE_URL + "/v2/workouts/%s/%s" % (
                    listStart.strftime(limitDateFormat),
                    listEnd.strftime(limitDateFormat)),
                headers=headers)

            for act in resp.json():
                if not act.get("completed", True):
                    continue
                activity = UploadedActivity()
                activity.StartTime = dateutil.parser.parse(act["StartTime"]).replace(tzinfo=None)
                logger.debug("Activity s/t " + str(activity.StartTime))
                activity.EndTime = activity.StartTime + timedelta(hours=act["TotalTime"])
                activity.Name = act.get("Title", None)
                activity.Notes = act.get("Description", None)
                activity.Type = self._workoutTypeMappings.get(act.get("WorkoutType", "").lower(), ActivityType.Other)

                activity.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute,
                                                           avg=act.get("CadenceAverage", None),
                                                           max=act.get("CadenceMaximum", None))
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters,
                                                            value=act.get("Distance", None))
                activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters,
                                                             avg=act.get("ElevationAverage", None),
                                                             min=act.get("ElevationMinimum", None),
                                                             max=act.get("ElevationMaximum", None),
                                                             gain=act.get("ElevationGain", None),
                                                             loss=act.get("ElevationLoss", None))
                activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilojoules,
                                                          value=act.get("Energy", None))
                activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute,
                                                      avg=act.get("HeartRateAverage", None),
                                                      min=act.get("HeartRateMinimum", None),
                                                      max=act.get("HeartRateMaximum", None))
                activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts,
                                                         avg=act.get("PowerAverage", None),
                                                         max=act.get("PowerMaximum", None))
                activity.Stats.Temperature = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius,
                                                               avg=act.get("TemperatureAverage", None),
                                                               min=act.get("TemperatureMinimum", None),
                                                               max=act.get("TemperatureMaximum", None))
                activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond,
                                                         avg=act.get("VelocityAverage", None),
                                                         max=act.get("VelocityMaximum", None))
                activity.CalculateUID()
                activities.append(activity)

            if not exhaustive_start_time:
                break

            listStart -= listStep
            listEnd -= listStep
            if listEnd < totalListStart:
                break

        return activities, exclusions

    def UploadActivity(self, svcRecord, activity):
        pwxdata_gz = BytesIO()
        with gzip.GzipFile(fileobj=pwxdata_gz, mode="w") as gzf:
          gzf.write(PWXIO.Dump(activity).encode("utf-8"))

        headers = self._apiHeaders(svcRecord)
        headers.update({"Content-Type": "application/json"})
        data = {
            "UploadClient": "tapiriik",
            "Filename": "tap-%s.pwx" % activity.UID,
            "SetWorkoutPublic": not activity.Private,
            # NB activity notes and name are in the PWX.
            "Data": base64.b64encode(pwxdata_gz.getvalue()).decode("ascii")
        }

        initiate_resp = requests.post(TRAININGPEAKS_API_BASE_URL + "/v3/file", data=json.dumps(data), headers=headers, allow_redirects=False)
        if initiate_resp.status_code != 202:
            raise APIException("Unable to initiate activity upload, response " + initiate_resp.text + " status " + str(initiate_resp.status_code))
        while True:
            time.sleep(5)
            check_resp = requests.get(initiate_resp.headers["Location"], headers=headers)
            if check_resp.status_code == 422:
                return None # Duplicate - didn't upload anything and don't need to retry    
            if check_resp.status_code != 200:
                raise APIException("Unable to check activity upload, response " + check_resp.text + " status " + str(check_resp.status_code))
            check_result = check_resp.json()
            if check_result["Completed"]:
                break
        if not len(check_result["WorkoutIds"]):
            raise APIException("Unable to upload activity, response " + str(check_result))
        return check_result["WorkoutIds"][0]
