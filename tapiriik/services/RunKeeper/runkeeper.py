from tapiriik.settings import WEB_ROOT, RUNKEEPER_CLIENT_ID, RUNKEEPER_CLIENT_SECRET, AGGRESSIVE_CACHE
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.stream_sampling import StreamSampler
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, WaypointType, Waypoint, Location, Lap
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import urllib.parse
import json
import logging
logger = logging.getLogger(__name__)


class RunKeeperService(ServiceBase):
    ID = "runkeeper"
    DisplayName = "RunKeeper"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://runkeeper.com/user/{0}/profile"
    AuthenticationNoFrame = True  # Chrome update broke this

    _activityMappings = {"Running": ActivityType.Running,
                         "Cycling": ActivityType.Cycling,
                         "Mountain Biking": ActivityType.MountainBiking,
                         "Walking": ActivityType.Walking,
                         "Hiking": ActivityType.Hiking,
                         "Downhill Skiing": ActivityType.DownhillSkiing,
                         "Cross-Country Skiing": ActivityType.CrossCountrySkiing,
                         "Snowboarding": ActivityType.Snowboarding,
                         "Skating": ActivityType.Skating,
                         "Swimming": ActivityType.Swimming,
                         "Wheelchair": ActivityType.Wheelchair,
                         "Rowing": ActivityType.Rowing,
                         "Elliptical": ActivityType.Elliptical,
                         "Other": ActivityType.Other}
    SupportedActivities = list(_activityMappings.values())

    SupportsHR = True
    SupportsCalories = True

    _wayptTypeMappings = {"start": WaypointType.Start, "end": WaypointType.End, "pause": WaypointType.Pause, "resume": WaypointType.Resume}

    def WebInit(self):
        self.UserAuthorizationURL = "https://runkeeper.com/apps/authorize?client_id=" + RUNKEEPER_CLIENT_ID + "&response_type=code&redirect_uri=" + WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service

        #  might consider a real OAuth client
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": RUNKEEPER_CLIENT_ID, "client_secret": RUNKEEPER_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})}

        response = requests.post("https://runkeeper.com/apps/token", data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if response.status_code != 200:
            raise APIException("Invalid code")
        token = response.json()["access_token"]

        # hacky, but also totally their fault for not giving the user id in the token req
        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Token": token})
        if existingRecord is None:
            uid = self._getUserId(ServiceRecord({"Authorization": {"Token": token}}))  # meh
        else:
            uid = existingRecord.ExternalID

        return (uid, {"Token": token})

    def RevokeAuthorization(self, serviceRecord):
        resp = requests.post("https://runkeeper.com/apps/de-authorize", data={"access_token": serviceRecord.Authorization["Token"]})
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize RK auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "Bearer " + serviceRecord.Authorization["Token"]}

    def _getAPIUris(self, serviceRecord):
        if hasattr(self, "_uris"):  # cache these for the life of the batch job at least? hope so
            return self._uris
        else:
            response = requests.get("https://api.runkeeper.com/user/", headers=self._apiHeaders(serviceRecord))

            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve user URLs", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve user URLs" + str(response))

            uris = response.json()
            for k in uris.keys():
                if type(uris[k]) == str:
                    uris[k] = "https://api.runkeeper.com" + uris[k]
            self._uris = uris
            return uris

    def _getUserId(self, serviceRecord):
        resp = requests.get("https://api.runkeeper.com/user/", headers=self._apiHeaders(serviceRecord))
        data = resp.json()
        return data["userID"]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        uris = self._getAPIUris(serviceRecord)

        allItems = []

        pageUri = uris["fitness_activities"]

        while True:
            response = requests.get(pageUri, headers=self._apiHeaders(serviceRecord))
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve activity list " + str(response) + " " + response.text)
            data = response.json()
            allItems += data["items"]
            if not exhaustive or "next" not in data or data["next"] == "":
                break
            pageUri = "https://api.runkeeper.com" + data["next"]

        activities = []
        exclusions = []
        for act in allItems:
            try:
                activity = self._populateActivity(act)
            except KeyError as e:
                exclusions.append(APIExcludeActivity("Missing key in activity data " + str(e), activityId=act["uri"]))
                continue

            logger.debug("\tActivity s/t " + str(activity.StartTime))
            if (activity.StartTime - activity.EndTime).total_seconds() == 0:
                exclusions.append(APIExcludeActivity("0-length", activityId=act["uri"]))
                continue  # these activites are corrupted
            activity.ServiceData = {"ActivityID": act["uri"]}
            activities.append(activity)
        return activities, exclusions

    def _populateActivity(self, rawRecord):
        ''' Populate the 1st level of the activity object with all details required for UID from RK API data '''
        activity = UploadedActivity()
        #  can stay local + naive here, recipient services can calculate TZ as required
        activity.StartTime = datetime.strptime(rawRecord["start_time"], "%a, %d %b %Y %H:%M:%S")
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(0, float(rawRecord["duration"]))) # P. sure this is moving time
        activity.EndTime = activity.StartTime + activity.Stats.MovingTime.Value # this is inaccurate with pauses - excluded from hash
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=rawRecord["total_distance"])
        # I'm fairly sure this is how the RK calculation works. I remember I removed something exactly like this from ST.mobi, but I trust them more than I trust myself to get the speed right.
        if (activity.EndTime - activity.StartTime).total_seconds() > 0:
            activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value / ((activity.EndTime - activity.StartTime).total_seconds() / 60 / 60))
        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=rawRecord["total_calories"] if "total_calories" in rawRecord else None)
        if rawRecord["type"] in self._activityMappings:
            activity.Type = self._activityMappings[rawRecord["type"]]
        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        activityID = activity.ServiceData["ActivityID"]
        if AGGRESSIVE_CACHE:
            ridedata = cachedb.rk_activity_cache.find_one({"uri": activityID})
        if not AGGRESSIVE_CACHE or ridedata is None:
            response = requests.get("https://api.runkeeper.com" + activityID, headers=self._apiHeaders(serviceRecord))
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to download activity" + activityID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to download activity " + activityID + " response " + str(response) + " " + response.text)
            ridedata = response.json()
            ridedata["Owner"] = serviceRecord.ExternalID
            if AGGRESSIVE_CACHE:
                cachedb.rk_activity_cache.insert(ridedata)

        if "is_live" in ridedata and ridedata["is_live"] is True:
            raise APIExcludeActivity("Not complete", activityId=activityID, permanent=False)

        if "userID" in ridedata and int(ridedata["userID"]) != int(serviceRecord.ExternalID):
            raise APIExcludeActivity("Not the user's own activity", activityId=activityID)

        self._populateActivityWaypoints(ridedata, activity)

        if "climb" in ridedata:
            activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(ridedata["climb"]))
        if "average_heart_rate" in ridedata:
            activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(ridedata["average_heart_rate"]))
        activity.Stationary = activity.CountTotalWaypoints() <= 1

        # This could cause confusion, since when I upload activities to RK I populate the notes field with the activity name. My response is to... well... not sure.
        activity.Notes = ridedata["notes"] if "notes" in ridedata else None
        activity.Private = ridedata["share"] == "Just Me"
        return activity

    def _populateActivityWaypoints(self, rawData, activity):
        ''' populate the Waypoints collection from RK API data '''
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        print(rawData.keys())
        streamData = {}
        for stream in ["path", "heart_rate", "calories", "distance"]:
            if stream in rawData and len(rawData[stream]):
                if stream == "path":
                    # The path stream doesn't follow the same naming convention, so we cheat and put everything in.
                    streamData[stream] = [(x["timestamp"], x) for x in rawData[stream]]
                else:
                    streamData[stream] = [(x["timestamp"], x[stream]) for x in rawData[stream]] # Change up format for StreamSampler

        def _addWaypoint(timestamp, path=None, heart_rate=None, calories=None, distance=None):
            waypoint = Waypoint(activity.StartTime + timedelta(seconds=timestamp))
            if path:
                waypoint.Location = Location(path["latitude"], path["longitude"], path["altitude"] if "altitude" in path and float(path["altitude"]) != 0 else None)  # if you're running near sea level, well...
                waypoint.Type = self._wayptTypeMappings[path["type"]] if path["type"] in self._wayptTypeMappings else WaypointType.Regular
            waypoint.HR = heart_rate
            waypoint.Calories = calories
            waypoint.Distance = distance

            lap.Waypoints.append(waypoint)
        activity.Stationary = len(lap.Waypoints) == 0
        if not activity.Stationary:
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End

        StreamSampler.SampleWithCallback(_addWaypoint, streamData)

    def UploadActivity(self, serviceRecord, activity):
        #  assembly dict to post to RK
        uploadData = self._createUploadData(activity)
        uris = self._getAPIUris(serviceRecord)
        headers = self._apiHeaders(serviceRecord)
        headers["Content-Type"] = "application/vnd.com.runkeeper.NewFitnessActivity+json"
        response = requests.post(uris["fitness_activities"], headers=headers, data=json.dumps(uploadData))

        if response.status_code != 201:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to upload activity " + activity.UID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to upload activity " + activity.UID + " response " + str(response) + " " + response.text)

    def _createUploadData(self, activity):
        ''' create data dict for posting to RK API '''
        record = {}

        record["type"] = [key for key in self._activityMappings if self._activityMappings[key] == activity.Type][0]
        record["start_time"] = activity.StartTime.strftime("%a, %d %b %Y %H:%M:%S")
        record["duration"] = activity.Stats.MovingTime.Value.total_seconds() if activity.Stats.MovingTime.Value else (activity.Stats.TimerTime.Value.total_seconds() if activity.Stats.TimerTime.Value else (activity.EndTime - activity.StartTime).total_seconds())
        if activity.Stats.HR.Average is not None:
            record["average_heart_rate"] = int(activity.Stats.HR.Average)
        if activity.Stats.Energy.Value is not None:
            record["total_calories"] = activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value
        if activity.Stats.Distance.Value is not None:
            record["total_distance"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value
        if activity.Name:
            record["notes"] = activity.Name  # not symetric, but better than nothing
        if activity.Private:
            record["share"] = "Just Me"

        if activity.CountTotalWaypoints() > 1:
            inPause = False
            for lap in activity.Laps:
                for waypoint in lap.Waypoints:
                    timestamp = (waypoint.Timestamp - activity.StartTime).total_seconds()

                    if waypoint.Type in self._wayptTypeMappings.values():
                        wpType = [key for key, value in self._wayptTypeMappings.items() if value == waypoint.Type][0]
                    else:
                        wpType = "gps"  # meh

                    if not inPause and waypoint.Type == WaypointType.Pause:
                        inPause = True
                    elif inPause and waypoint.Type == WaypointType.Pause:
                        continue # RK gets all crazy when you send it multiple pause waypoints in a row.
                    elif inPause and waypoint.Type != WaypointType.Pause:
                        inPause = False

                    if waypoint.Location is not None and waypoint.Location.Latitude is not None and waypoint.Location.Longitude is not None:
                        if "path" not in record:
                            record["path"] = []
                        pathPt = {"timestamp": timestamp,
                                  "latitude": waypoint.Location.Latitude,
                                  "longitude": waypoint.Location.Longitude,
                                  "type": wpType}
                        pathPt["altitude"] = waypoint.Location.Altitude if waypoint.Location.Altitude is not None else 0  # this is straight of of their "example calls" page
                        record["path"].append(pathPt)

                    if waypoint.HR is not None:
                        if "heart_rate" not in record:
                            record["heart_rate"] = []
                        record["heart_rate"].append({"timestamp": timestamp, "heart_rate": round(waypoint.HR)})

                    if waypoint.Calories is not None:
                        if "calories" not in record:
                            record["calories"] = []
                        record["calories"].append({"timestamp": timestamp, "calories": waypoint.Calories})

                    if waypoint.Distance is not None:
                        if "distance" not in record:
                            record["distance"] = []
                        record["distance"].append({"timestamp": timestamp, "distance": waypoint.Distance})

        return record

    def DeleteCachedData(self, serviceRecord):
        cachedb.rk_activity_cache.remove({"Owner": serviceRecord.ExternalID})
