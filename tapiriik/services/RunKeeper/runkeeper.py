from tapiriik.settings import WEB_ROOT, RUNKEEPER_CLIENT_ID, RUNKEEPER_CLIENT_SECRET, AGGRESSIVE_CACHE
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.stream_sampling import StreamSampler
from tapiriik.services.auto_pause import AutoPauseCalculator
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, WaypointType, Waypoint, Location, Lap
from tapiriik.database import cachedb, redis
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import urllib.parse
import json
import logging
import re
logger = logging.getLogger(__name__)


class RunKeeperService(ServiceBase):
    ID = "runkeeper"
    DisplayName = "Runkeeper"
    DisplayAbbreviation = "RK"
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
                         "Strength Training": ActivityType.StrengthTraining,
                         "Other": ActivityType.Other}
    SupportedActivities = list(_activityMappings.values())

    SupportsHR = True
    SupportsCalories = True

    _wayptTypeMappings = {"start": WaypointType.Start, "end": WaypointType.End, "pause": WaypointType.Pause, "resume": WaypointType.Resume}
    _URI_CACHE_KEY = "rk:user_uris"
    _RATE_LIMIT_KEY = "rk:rate_limit:%s:hit"

    def _rate_limit(self, endpoint, req_lambda):
        if redis.get(self._RATE_LIMIT_KEY % endpoint) is not None:
            raise APIException("RK global rate limit previously reached on %s" % endpoint, user_exception=UserException(UserExceptionType.RateLimited))
        response = req_lambda()
        if response.status_code == 429:
            if "user" not in response.text:
                # When we hit a limit we preemptively fail all future requests till we're sure
                # than the limit is expired. The maximum period appears to be 1 day.
                # This entire thing is an excercise in better-safe-than-sorry as it's unclear
                # how their rate-limit logic works (when limits reset, etc).

                # As it turns out, there are several parallel rate limits operating at once.
                # Attempt to parse out how long we should wait - if we can't figure it out,
                # default to the shortest time I've seen (15m). As long as the timer doesn't reset
                # every time you request during the over-quota period, this should work.
                timeout = timedelta(minutes=15)
                timeout_match = re.search(r"(\d+) (second|minute|hour|day)", response.text)
                if timeout_match:
                    # This line is too clever for its own good.
                    timeout = timedelta(**{"%ss" % timeout_match.group(2): float(timeout_match.group(1))})

                redis.setex(self._RATE_LIMIT_KEY % endpoint, response.text, timeout)
                raise APIException("RK global rate limit reached on %s" % endpoint, user_exception=UserException(UserExceptionType.RateLimited))
            else:
                # Per-user limit hit: don't halt entire system, just bail for this user
                # If a user has too many pages of activities, they will never sync as we'll keep hitting the limit.
                # But that's a Very Hard Problem to Solve™ given that I can't do incremental listings...
                raise APIException("RK user rate limit reached on %s" % endpoint, user_exception=UserException(UserExceptionType.RateLimited))
        return response

    def WebInit(self):
        self.UserAuthorizationURL = "https://runkeeper.com/apps/authorize?client_id=" + RUNKEEPER_CLIENT_ID + "&response_type=code&redirect_uri=" + WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service

        #  might consider a real OAuth client
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": RUNKEEPER_CLIENT_ID, "client_secret": RUNKEEPER_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})}

        response = self._rate_limit("auth_token",
                                    lambda: requests.post("https://runkeeper.com/apps/token",
                                                          data=urllib.parse.urlencode(params),
                                                          headers={"Content-Type": "application/x-www-form-urlencoded"}))

        if response.status_code != 200:
            raise APIException("Invalid code")
        token = response.json()["access_token"]

        # This used to check with GetServiceRecordWithAuthDetails but that's hideously slow on an unindexed field.
        uid = self._getUserId(ServiceRecord({"Authorization": {"Token": token}}))  # meh

        return (uid, {"Token": token})

    def RevokeAuthorization(self, serviceRecord):
        resp = self._rate_limit("revoke_token",
                                lambda: requests.post("https://runkeeper.com/apps/de-authorize",
                                                      data={"access_token": serviceRecord.Authorization["Token"]}))
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize RK auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "Bearer " + serviceRecord.Authorization["Token"],
                "Accept-Charset": "UTF-8"}

    def _getAPIUris(self, serviceRecord):
        if hasattr(self, "_uris"):  # cache these for the life of the batch job at least? hope so
            return self._uris
        else:
            uris_json = redis.get(self._URI_CACHE_KEY)
            if uris_json is not None:
                uris = json.loads(uris_json.decode('utf-8'))
            else:
                response = self._rate_limit("user",
                                            lambda: requests.get("https://api.runkeeper.com/user/",
                                                                 headers=self._apiHeaders(serviceRecord)))
                if response.status_code != 200:
                    if response.status_code == 401 or response.status_code == 403:
                        raise APIException("No authorization to retrieve user URLs", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                    raise APIException("Unable to retrieve user URLs" + str(response))

                uris = response.json()
                for k in uris.keys():
                    if type(uris[k]) == str:
                        uris[k] = "https://api.runkeeper.com" + uris[k]
                # Runkeeper wants you to request these on a per-user basis.
                # In practice, the URIs are identical for ever user (only the userID key changes).
                # So, only do it once every 24 hours, across the entire system.
                redis.setex(self._URI_CACHE_KEY, json.dumps(uris), timedelta(hours=24))
            self._uris = uris
            return uris

    def _getUserId(self, serviceRecord):
        resp = self._rate_limit("user",
                                lambda: requests.get("https://api.runkeeper.com/user/",
                                                     headers=self._apiHeaders(serviceRecord)))
        if resp.status_code != 200:
            raise APIException("Failed to retrieve RK user metadata %s: %s" % (resp.status_code, resp.text))
        data = resp.json()
        return data["userID"]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        uris = self._getAPIUris(serviceRecord)

        allItems = []

        pageUri = uris["fitness_activities"]

        while True:
            response = self._rate_limit("list",
                                        lambda: requests.get(pageUri,
                                                             headers=self._apiHeaders(serviceRecord)))
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
                exclusions.append(APIExcludeActivity("Missing key in activity data " + str(e), activity_id=act["uri"], user_exception=UserException(UserExceptionType.Corrupt)))
                continue

            logger.debug("\tActivity s/t " + str(activity.StartTime))
            if (activity.StartTime - activity.EndTime).total_seconds() == 0:
                exclusions.append(APIExcludeActivity("0-length", activity_id=act["uri"]))
                continue  # these activites are corrupted
            activity.ServiceData = {"ActivityID": act["uri"]}
            activities.append(activity)
        return activities, exclusions

    def _populateActivity(self, rawRecord):
        ''' Populate the 1st level of the activity object with all details required for UID from RK API data '''
        activity = UploadedActivity()
        #  can stay local + naive here, recipient services can calculate TZ as required
        activity.StartTime = datetime.strptime(rawRecord["start_time"], "%a, %d %b %Y %H:%M:%S")
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(rawRecord["duration"])) # P. sure this is moving time
        activity.EndTime = activity.StartTime + timedelta(seconds=float(rawRecord["duration"])) # this is inaccurate with pauses - excluded from hash
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=rawRecord["total_distance"])
        # I'm fairly sure this is how the RK calculation works. I remember I removed something exactly like this from ST.mobi, but I trust them more than I trust myself to get the speed right.
        if (activity.EndTime - activity.StartTime).total_seconds() > 0:
            activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value / ((activity.EndTime - activity.StartTime).total_seconds() / 60 / 60))
        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=rawRecord["total_calories"] if "total_calories" in rawRecord else None)
        if rawRecord["type"] in self._activityMappings:
            activity.Type = self._activityMappings[rawRecord["type"]]
        activity.GPS = rawRecord["has_path"] and rawRecord['tracking_mode'] == "outdoor"
        activity.Stationary = not rawRecord["has_path"]
        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        activityID = activity.ServiceData["ActivityID"]
        if AGGRESSIVE_CACHE:
            ridedata = cachedb.rk_activity_cache.find_one({"uri": activityID})
        if not AGGRESSIVE_CACHE or ridedata is None:
            response = self._rate_limit("download",
                                        lambda: requests.get("https://api.runkeeper.com" + activityID,
                                                             headers=self._apiHeaders(serviceRecord)))
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to download activity" + activityID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to download activity " + activityID + " response " + str(response) + " " + response.text)
            ridedata = response.json()
            ridedata["Owner"] = serviceRecord.ExternalID
            if AGGRESSIVE_CACHE:
                cachedb.rk_activity_cache.insert(ridedata)

        if "is_live" in ridedata and ridedata["is_live"] is True:
            raise APIExcludeActivity("Not complete", activity_id=activityID, permanent=False, user_exception=UserException(UserExceptionType.LiveTracking))

        if "userID" in ridedata and int(ridedata["userID"]) != int(serviceRecord.ExternalID):
            raise APIExcludeActivity("Not the user's own activity", activity_id=activityID, user_exception=UserException(UserExceptionType.Other))

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
                if path["latitude"] != 0 and path["longitude"] != 0:
                    waypoint.Location = Location(path["latitude"], path["longitude"], path["altitude"] if "altitude" in path and float(path["altitude"]) != 0 else None)  # if you're running near sea level, well...
                waypoint.Type = self._wayptTypeMappings[path["type"]] if path["type"] in self._wayptTypeMappings else WaypointType.Regular
            waypoint.HR = heart_rate
            waypoint.Calories = calories
            waypoint.Distance = distance

            lap.Waypoints.append(waypoint)
        StreamSampler.SampleWithCallback(_addWaypoint, streamData)

        activity.Stationary = len(lap.Waypoints) == 0
        activity.GPS = any(wp.Location and wp.Location.Longitude is not None and wp.Location.Latitude is not None for wp in lap.Waypoints)
        if not activity.Stationary:
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End

    def UploadActivity(self, serviceRecord, activity):
        #  assembly dict to post to RK
        uploadData = self._createUploadData(activity, serviceRecord.GetConfiguration()["auto_pause"])
        uris = self._getAPIUris(serviceRecord)
        headers = self._apiHeaders(serviceRecord)
        headers["Content-Type"] = "application/vnd.com.runkeeper.NewFitnessActivity+json"
        response = self._rate_limit("upload",
                                    lambda: requests.post(uris["fitness_activities"],
                                                          headers=headers,
                                                          data=json.dumps(uploadData)))

        if response.status_code != 201:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to upload activity " + activity.UID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to upload activity " + activity.UID + " response " + str(response) + " " + response.text)
        return response.headers["location"]

    def _createUploadData(self, activity, auto_pause=False):
        ''' create data dict for posting to RK API '''
        record = {}

        record["type"] = [key for key in self._activityMappings if self._activityMappings[key] == activity.Type][0]
        record["start_time"] = activity.StartTime.strftime("%a, %d %b %Y %H:%M:%S")
        if activity.Stats.MovingTime.Value is not None:
            record["duration"] = activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        elif activity.Stats.TimerTime.Value is not None:
            record["duration"] = activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        else:
            record["duration"] = (activity.EndTime - activity.StartTime).total_seconds()

        if activity.Stats.HR.Average is not None:
            record["average_heart_rate"] = int(activity.Stats.HR.Average)
        if activity.Stats.Energy.Value is not None:
            record["total_calories"] = activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value
        if activity.Stats.Distance.Value is not None:
            record["total_distance"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value

        if activity.Name and activity.Notes and activity.Name != activity.Notes:
            record["notes"] = activity.Name + " - " + activity.Notes
        elif activity.Notes:
            record["notes"] = activity.Notes
        elif activity.Name:
            record["notes"] = activity.Name

        if activity.Private:
            record["share"] = "Just Me"

        if activity.CountTotalWaypoints() > 1:
            flat_wps = activity.GetFlatWaypoints()

            anchor_ts = flat_wps[0].Timestamp

            # By default, use the provided waypoint types
            wp_type_iter = (wp.Type for wp in flat_wps)
            # Unless those types don't include pause/resume, in which case use our auto-pause calculation
            if auto_pause and not any(wp.Type == WaypointType.Pause for wp in flat_wps):
                # ...but not if we don't know the intended moving time
                if activity.Stats.MovingTime.Value:
                    wp_type_iter = AutoPauseCalculator.calculate(flat_wps, activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value)

            inPause = False
            for waypoint, waypoint_type in zip(flat_wps, wp_type_iter):
                timestamp = (waypoint.Timestamp - anchor_ts).total_seconds()

                if waypoint_type in self._wayptTypeMappings.values():
                    wpType = [key for key, value in self._wayptTypeMappings.items() if value == waypoint_type][0]
                else:
                    wpType = "gps"  # meh

                if not inPause and waypoint_type == WaypointType.Pause:
                    inPause = True
                elif inPause and waypoint_type == WaypointType.Pause:
                    continue # RK gets all crazy when you send it multiple pause waypoints in a row.
                elif inPause and waypoint_type != WaypointType.Pause:
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

    def DeleteActivity(self, serviceRecord, uri):
        headers = self._apiHeaders(serviceRecord)
        del_res = self._rate_limit("delete",
                                   lambda: requests.delete("https://api.runkeeper.com/%s" % uri,
                                                           headers=headers))
        del_res.raise_for_status()

