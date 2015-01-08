from tapiriik.settings import WEB_ROOT, LFCONNECT_CLIENT_ID, LFCONNECT_CLIENT_SECRET, AGGRESSIVE_CACHE
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.stream_sampling import StreamSampler
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, WaypointType, Waypoint, Location, Lap
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode
import requests
import json
import logging
import dateutil.parser
logger = logging.getLogger(__name__)


class LFConnectService(ServiceBase):
    ID = "lfconnect"
    DisplayName = "LFConnect"
    DisplayAbbreviation = "LC"
    AuthenticationType = ServiceAuthenticationType.OAuth

# QA Service
#    BaseURL = "https://vtqa.lfconnect.com/web/"

# Production Service - requires a production API key
    BaseURL = "https://api.lfopen.lfconnect.com/web/"

    UserProfileURL = BaseURL + "api2/user"
    AuthenticationNoFrame = True  # Chrome update broke this

    _activityMappingsIn = {"F3 Treadmill": ActivityType.Running,
                           "T3 Treadmill": ActivityType.Running,
                           "T5 Treadmill": ActivityType.Running,
                           "C1 Lifecycle": ActivityType.Cycling,
                           "C3 Lifecycle": ActivityType.Cycling,
                           "RS1 Lifecycle": ActivityType.Cycling,
                           "RS3 Lifecycle": ActivityType.Cycling,
                           "Other": ActivityType.Other}
    _activityMappingsOut = {ActivityType.Running: "Running",
                            ActivityType.Cycling: "Cycling",
                            ActivityType.MountainBiking: "MtnBiking",
                            ActivityType.Walking: "Walking",
                            ActivityType.Hiking: "Hiking",
                            ActivityType.DownhillSkiing: "DownhillSkiing",
                            ActivityType.CrossCountrySkiing: "XCSkiing",
                            ActivityType.Snowboarding: "Snowboarding",
                            ActivityType.Skating: "Skating",
                            ActivityType.Swimming: "Swimming",
                            ActivityType.Wheelchair: "Wheelchair",
                            ActivityType.Rowing: "Rowing",
                            ActivityType.Elliptical: "Elliptical",
                            ActivityType.Gym: "Gym",
                            ActivityType.Climbing: "Climbing",
                            ActivityType.Other: "Other"}
    SupportedActivities = list(_activityMappingsOut.keys())

    SupportsHR = True
    SupportsCalories = True

#    _wayptTypeMappings = {"start": WaypointType.Start, "end": WaypointType.End, "pause": WaypointType.Pause, "resume": WaypointType.Resume}

    def WebInit(self):
        self.UserAuthorizationURL = self.BaseURL + "oauthAuthorize?client_id=" + LFCONNECT_CLIENT_ID + "&response_type=code&redirect_uri=" + WEB_ROOT + reverse("oauth_return", kwargs={"service": "lfconnect"})

    def _apiParams(self, serviceRecord):
        from tapiriik.auth.credential_storage import CredentialStore
        return {"access_token": serviceRecord.Authorization["Token"]}

    def _apiHeaders(self, serviceRecord):
        return {"Accept": "application/json"}

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service

        code = req.GET.get("code")

        params = {"grant_type": "authorization_code", 
                  "code": code, 
                  "client_id": LFCONNECT_CLIENT_ID, 
                  "client_secret": LFCONNECT_CLIENT_SECRET, 
                  "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "lfconnect"})}

        response = requests.post(self.BaseURL + "authorizeresponse", data=urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"},allow_redirects=False,verify=False)
        if response.status_code != 302:
            raise APIException("Invalid code" + response.text + response.url)

        redirect = response.headers['location']
        query = urlparse(redirect).query
        token = parse_qs(query)['access_token'][0]

        # hacky, but also totally their fault for not giving the user id in the token req
        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Token": token})
        if existingRecord is None:
            uid = self._getUserId(ServiceRecord({"Authorization": {"Token": token}}))  # meh
        else:
            uid = existingRecord.ExternalID

        return (uid, {"Token": token})

    def _revokeParams(self, serviceRecord):
        from tapiriik.auth.credential_storage import CredentialStore
        return {"token": serviceRecord.Authorization["Token"]}

    def RevokeAuthorization(self, serviceRecord):
        resp = requests.post(self.BaseURL + "revoke", data=self._revokeParams(serviceRecord), headers=self._apiHeaders(serviceRecord),verify=False)
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize LFConnect auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _getUserId(self, serviceRecord):
        resp = requests.get(self.BaseURL + "api2/user", params=self._apiParams(serviceRecord), headers=self._apiHeaders(serviceRecord),verify=False)
        data = resp.json()
        return data["emailAddress"]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        pageUri = self.BaseURL + "api2/workoutresults/get_lifefitness_results"

        reqData = self._apiParams(serviceRecord)

        limitDateFormat = "%m/%d/%Y"

        if exhaustive:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = datetime(day=1, month=1, year=1980) # The beginning of time
        else:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = listEnd - timedelta(days=20) # Doesn't really matter

        reqData.update({"fromDate": listStart.strftime(limitDateFormat), "toDate": listEnd.strftime(limitDateFormat), "timezone": "GMT-0"})
        response = requests.get(pageUri, params=reqData, headers=self._apiHeaders(serviceRecord),verify=False)
        if response.status_code != 200:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to retrieve activity list " + str(response) + " " + response.text)

        try:
            data = response.json()
        except (ValueError, TypeError):
            raise APIException("Unable to retrieve activity list " + response.text)

        activities = []
        exclusions = []
        if (data is None):
            return activities, exclusions

        for act in data["resultWorkoutData"]:
            try:
                activity = self._populateActivity(act)
            except KeyError as e:
                exclusions.append(APIExcludeActivity("Missing key in activity data " + str(e), activityId=act["cardioDataDetails"][0]["date"], userException=UserException(UserExceptionType.Corrupt)))
                continue

            logger.debug("\tActivity s/t " + str(activity.StartTime))
            if (activity.StartTime - activity.EndTime).total_seconds() == 0:
                exclusions.append(APIExcludeActivity("0-length", activityId=act["cardioDataDetails"][0]["date"]))
                continue  # these activites are corrupted
            activity.ServiceData = {"ActivityID": act["cardioDataDetails"][0]["date"]}
            activities.append(activity)
        return activities, exclusions

    def _populateActivity(self, rawRecord):
        activity = UploadedActivity()
        cardioDataDetails = rawRecord["cardioDataDetails"]
        activity.StartTime = dateutil.parser.parse(cardioDataDetails[0]["date"])
        activity.EndTime = activity.StartTime + timedelta(minutes=float(rawRecord["duration"])) # this is inaccurate with pauses - excluded from hash
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Minutes, value = float(rawRecord["duration"])) 
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value = float(rawRecord["distance"]))
        activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=(activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value / activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Minutes).Value) / 60)
        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(rawRecord["calorie"]))
        activity.Type = ActivityType.Other

        if rawRecord["equipmentName"] in self._activityMappingsIn:
            activity.Type = self._activityMappingsIn[rawRecord["equipmentName"]]
        else:
            logger.info("New LifeFitness equipment name %s should be mapped to an activity type" % rawRecord["equipmentName"])

        activity.Notes = rawRecord["equipmentName"] if "equipmentName" in rawRecord else None
        activity.GPS = False
        activity.Stationary = True
        self._populateActivityWaypoints(cardioDataDetails, activity)

        activity.CalculateUID()
        return activity

    def _populateActivityWaypoints(self, cardioDataDetails, activity):
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        streamData = {}
        for cardioDataDetail in cardioDataDetails:
            for stream in ["valueChangeHeartRate", "valueChangeCalories", "valueChangeDistance", "valueChangeSpeed"]:
                if stream in cardioDataDetail and len(cardioDataDetail[stream]):
                        valueChange = [float(x) for x in cardioDataDetail[stream]["valueChange"].split(',')]
                        timeChange = [int(x) for x in cardioDataDetail[stream]["timeChange"].split(',')]
                        streamData[stream] = list(zip(timeChange, valueChange))

        if "valueChangeHeartRate" in streamData:
            heartRates = []
            for timestamp, heartRate in streamData["valueChangeHeartRate"]:
                heartRates.append(heartRate)
            activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=sum(heartRates)/len(heartRates), max=max(heartRates), min=min(heartRates))

        def _addWaypoint(timestamp, valueChangeHeartRate=None, valueChangeCalories=None, valueChangeDistance=None, valueChangeSpeed=None):
            waypoint = Waypoint(activity.StartTime + timedelta(seconds=timestamp))
            waypoint.Type = WaypointType.Regular
            waypoint.HR = valueChangeHeartRate
            waypoint.Calories = valueChangeCalories
            waypoint.Distance = valueChangeDistance
            waypoint.Speed = valueChangeSpeed
            lap.Waypoints.append(waypoint)

        StreamSampler.SampleWithCallback(_addWaypoint, streamData)
        if activity.CountTotalWaypoints() > 0:
            activity.Stationary = False
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End


    # Populated above, no need
    def DownloadActivity(self, serviceRecord, activity):
        return activity

    def _createUploadData(self, activity):
        def _formatTime(t):
            s1 = t.strftime("%Y-%m-%dT%H:%M:%S")
            s2 = t.strftime(".%f")
            s3 = t.strftime("%z")
            s4 = s3[:3] + ":" + s3[3:]
            f = round(float(s2), 3)
            return "%s.%03d%s" % (s1, f * 1000, s4)
   
        ''' create data dict for posting to RK API '''
        record = {}
  
        record["name"] = self._activityMappingsOut[activity.Type]
        # Format: 2011-02-12T17:16:10.180+08:00 (yyyy-MM-ddTHH:mm:ss.SSS+hh:mm)
        record["datePerformed"] = _formatTime(activity.StartTime)
        record["preferredUnit"] = "M"

        if activity.Stats.MovingTime.Value is not None:
            record["time"] = activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Minutes).Value
        elif activity.Stats.TimerTime.Value is not None:
            record["time"] = activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Minutes).Value
        else:
            record["time"] = (activity.EndTime - activity.StartTime).total_seconds()

        if activity.Stats.Energy.Value is not None:
            record["calories"] = activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value
        else:
            record["calories"] = 0.0

        if activity.Stats.Distance.Value is not None:
            record["distance"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value
        else:
            record["distance"] = 0.0

        if activity.Notes:
            record["notes"] = activity.Notes

        if activity.Private:
            record["facebookSharing"] = False

        return record

    def UploadActivity(self, serviceRecord, activity):
        pageUri = self.BaseURL + "api2/workoutresults/postcardio"
        uploadData = self._createUploadData(activity)
        headers = self._apiHeaders(serviceRecord)
        headers["Content-Type"] = "application/json"
        response = requests.post(pageUri, headers=headers, data=json.dumps(uploadData),params=self._apiParams(serviceRecord),verify=False)

        if response.status_code != 200:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to upload activity " + activity.UID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to upload activity " + activity.UID + " response " + str(response) + " " + response.text)
        return uploadData["datePerformed"]

    def DeleteCachedData(self, serviceRecord):
        cachedb.rk_activity_cache.remove({"Owner": serviceRecord.ExternalID})

