from tapiriik.settings import PULSSTORY_CLIENT_ID, PULSSTORY_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.stream_sampling import StreamSampler
from tapiriik.services.auto_pause import AutoPauseCalculator
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, WaypointType, Waypoint, Location, Lap
from tapiriik.database import cachedb
from datetime import datetime, timedelta
import requests
import urllib.parse
import json
import logging
import collections
import zipfile
import io

logger = logging.getLogger(__name__)

class PulsstoryService(ServiceBase):
    ID = "pulsstory"
    DisplayName = "pulsstory"
    DisplayAbbreviation = "PLS"
    URLBase = 'https://www.pulsstory.com'
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = URLBase + "/user/{0}/profile"
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
    SupportsCadence = True
    SupportsPower = True

    _wayptTypeMappings = {"start": WaypointType.Start, "end": WaypointType.End, "pause": WaypointType.Pause, "resume": WaypointType.Resume}

    def WebInit(self):
        self.UserAuthorizationURL = self.URLBase + "/Account/LogOn?&ReturnUrl=/ExternalSyncAPI/GenerateCode"

    def RetrieveAuthorizationToken(self, req, level):
        #  might consider a real OAuth client
        code = req.GET.get("code")
        params = {"code": code, "client_id": PULSSTORY_CLIENT_ID, "client_secret": PULSSTORY_CLIENT_SECRET}

        response = requests.post(self.URLBase + "/ExternalSyncAPI/GenerateToken", data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if response.status_code != 200:
            raise APIException("Invalid code")

        token = response.json()["access_token"]

        # This used to check with GetServiceRecordWithAuthDetails but that's hideously slow on an unindexed field.
        uid = self._getUserId(ServiceRecord({"Authorization": {"Token": token}}))  # meh

        return (uid, {"Token": token})

    def RevokeAuthorization(self, serviceRecord):
        resp = requests.post(self.URLBase + "/ExternalSyncAPI/Deauthorize", data=self._apiData(serviceRecord))
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize pulsstory auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _apiData(self, serviceRecord):
        return {"access_token": serviceRecord.Authorization["Token"]}

    def _getAPIUris(self, serviceRecord):
        if hasattr(self, "_uris"):  # cache these for the life of the batch job at least? hope so
            return self._uris
        else:
            response = requests.post(self.URLBase + "/ExternalSyncAPI/Uris", data=self._apiData(serviceRecord))

            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve user URLs", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve user URLs" + str(response))

            uris = response.json()
            for k in uris.keys():
                if type(uris[k]) == str:
                    uris[k] = self.URLBase + uris[k]
            self._uris = uris
            return uris

    def _getUserId(self, serviceRecord):
        resp = requests.post(self.URLBase + "/ExternalSyncAPI/GetUserId", data=self._apiData(serviceRecord))
        if resp.status_code != 200:
            raise APIException("Unable to retrieve user id" + str(resp));
        data = resp.json()
        return data["userID"]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        uris = self._getAPIUris(serviceRecord)

        allItems = []

        pageUri = uris["fitness_activities"]

        while True:
            response = requests.post(pageUri, data=self._apiData(serviceRecord))
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve activity list " + str(response) + " " + response.text)
            data = response.json()

            allItems += data["Data"]["items"]
            if not exhaustive or "next" not in data["Data"] or data["Data"]["next"] == "":
                break
            pageUri = self.URLBase + data["Data"]["next"]

        activities = []
        exclusions = []
        for act in allItems:
            try:
                activity = self._populateActivity(act)
            except KeyError as e:
                exclusions.append(APIExcludeActivity("Missing key in activity data " + str(e), activity_id=act["URI"], user_exception=UserException(UserExceptionType.Corrupt)))
                continue

            logger.debug("\tActivity s/t " + str(activity.StartTime))
            activity.ServiceData = {"ActivityID": act["URI"]}
            activities.append(activity)
        return activities, exclusions

    def _populateActivity(self, rawRecord):
        ''' Populate the 1st level of the activity object with all details required for UID from pulsstory API data '''
        activity = UploadedActivity()
        #  can stay local + naive here, recipient services can calculate TZ as required
        activity.Name = rawRecord["Name"] if "Name" in rawRecord else None
        activity.StartTime = datetime.strptime(rawRecord["StartTime"], "%Y-%m-%d %H:%M:%S")
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(rawRecord["Duration"]))
        activity.EndTime = activity.StartTime + timedelta(seconds=float(rawRecord["Duration"]))
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=rawRecord["Distance"])
        if (activity.EndTime - activity.StartTime).total_seconds() > 0:
            activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value / ((activity.EndTime - activity.StartTime).total_seconds() / 60 / 60))
        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=rawRecord["Energy"] if "Energy" in rawRecord else None)
        if rawRecord["Type"] in self._activityMappings:
            activity.Type = self._activityMappings[rawRecord["Type"]]
        activity.GPS = rawRecord["HasPath"] if "HasPath" in rawRecord else False
        activity.Stationary = rawRecord["HasPoints"] if "HasPoints" in rawRecord else True
        activity.Notes = rawRecord["Notes"] if "Notes" in rawRecord else None
        activity.Private = rawRecord["Private"] if "Private" in rawRecord else True

        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        activityID = activity.ServiceData["ActivityID"]

        response = requests.post(self.URLBase + activityID, data=self._apiData(serviceRecord))
        if response.status_code != 200:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to download activity" + activityID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to download activity " + activityID + " response " + str(response) + " " + response.text)
        ridedata = response.json()
        ridedata["Owner"] = serviceRecord.ExternalID

        if "UserID" in ridedata and int(ridedata["UserID"]) != int(serviceRecord.ExternalID):
            raise APIExcludeActivity("Not the user's own activity", activity_id=activityID, user_exception=UserException(UserExceptionType.Other))

        self._populateActivityWaypoints(ridedata, activity)

        if "Climb" in ridedata:
            activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(ridedata["Climb"]))
        if "AvgHr" in ridedata:
            activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(ridedata["AvgHr"]))
        activity.Stationary = activity.CountTotalWaypoints() <= 1

        return activity

    def _convertList(self, streamData, streamDataKey, rawData, listName):
        timeListName = listName + "Time"
        valueListName = listName + "Value"
        check = timeListName is not None and timeListName in rawData
        check = check and valueListName is not None and valueListName in rawData
        if check:
            timeList = rawData[timeListName]
            valueList = rawData[valueListName]
            if timeList is not None and valueList is not None:
                if len(timeList) > 0:
                    result = list(zip(timeList, valueList))
                    streamData[streamDataKey] = result

    def _convertPathList(self, streamData, streamDataKey, rawData):
        result = []
        timeListName = "PathTime"
        longitudeListName = "LongitudePathValue"
        latitudeListName = "LatitudePathValue"
        altitudeListName = "AltitudePathValue"        

        check = timeListName is not None and timeListName in rawData
        check = check and longitudeListName in rawData
        check = check and latitudeListName in rawData
        if check:
            timeList = rawData[timeListName]
            longitudeList = rawData[longitudeListName]
            latitudeList = rawData[latitudeListName]
            if altitudeListName in rawData:
                altitudeList = rawData[altitudeListName]
            else:
                altitudeList = None

            if timeList is not None and longitudeList is not None and latitudeList is not None:
               Nt = len(timeList)
               if Nt > 0:
                   for n in range(Nt):
                       point = { "longitude" : longitudeList[n], "latitude": latitudeList[n] }
                       if altitudeList is not None:
                           point["altitude"] = altitudeList[n]

                       result.append((timeList[n], point))
                   streamData[streamDataKey] = result

    def _populateActivityWaypoints(self, rawData, activity):
        ''' populate the Waypoints collection from pulsstory API data '''
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        streamData = {}

        self._convertList(streamData, "heart_rate", rawData, "HeartRate")
        self._convertList(streamData, "distance", rawData, "Distance")
        self._convertList(streamData, "speed", rawData, "Speed")
        self._convertList(streamData, "power", rawData, "Power")
        self._convertList(streamData, "cadence", rawData, "Cadence")
        self._convertPathList(streamData, "path", rawData)

        def _addWaypoint(timestamp, path=None, heart_rate=None, power=None, distance=None, speed=None, cadence=None):
            waypoint = Waypoint(activity.StartTime + timedelta(seconds=timestamp))
            if path:
                if path["latitude"] != 0 and path["longitude"] != 0:
                    waypoint.Location = Location(path["latitude"], path["longitude"], path["altitude"] if "altitude" in path and float(path["altitude"]) != 0 else None)  # if you're running near sea level, well...
                waypoint.Type = WaypointType.Regular
            waypoint.HR = heart_rate
            waypoint.Distance = distance
            waypoint.Speed = speed
            waypoint.Cadence = cadence
            waypoint.Power = power
            lap.Waypoints.append(waypoint)

        StreamSampler.SampleWithCallback(_addWaypoint, streamData)

        activity.Stationary = len(lap.Waypoints) == 0
        activity.GPS = any(wp.Location and wp.Location.Longitude is not None and wp.Location.Latitude is not None for wp in lap.Waypoints)
        if not activity.Stationary:
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End

    def UploadActivity(self, serviceRecord, activity):
        duration = self._getDuration(activity)
        if duration is None or duration <= 0:
            raise APIException("Pulsstory does not support trainings without duration.", block=True, scope=ServiceExceptionScope.Activity, user_exception=UserException(UserExceptionType.InsufficientData))

        #  assembly dict to post to pulsstory
        uploadData = self._createUploadData(activity, False)
        uris = self._getAPIUris(serviceRecord)
        data = self._apiData(serviceRecord)
        headers={}

        jsonData = json.dumps(uploadData)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as myzip:
                myzip.writestr('activity.txt', jsonData, compress_type=zipfile.ZIP_DEFLATED)
        files = {"data": buffer.getvalue()}

        response = requests.post(uris["upload_activity_zip"], data=data, files=files, headers=headers)
        if response.status_code != 200:
            if response.status_code == 401 or response.status_code == 403:
                raise APIException("No authorization to upload activity " + activity.UID, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to upload activity " + activity.UID + " response " + str(response) + " " + response.text)

        return response.json()["Id"]

    def _getDuration(self, activity):
        if activity.Stats.MovingTime.Value is not None:
            return activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        elif activity.Stats.TimerTime.Value is not None:
            return activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        else:
            return (activity.EndTime - activity.StartTime).total_seconds()

    def _createUploadData(self, activity, auto_pause=False):
        ''' create data dict for posting to pulsstory API '''
        record = {}

        record["Basic"] = {
            "Name" : activity.Name,
            "Duration" : self._getDuration(activity),
            "Distance" : activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value,
            "StartTime": activity.StartTime.strftime("%Y-%m-%d %H:%M:%S"),
            "Type": activity.Type,
            "Energy": activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value,
            "Notes" : activity.Notes,
            "Private" : activity.Private,
            }

        waypoints = {
            "AvgHR" : activity.Stats.HR.Average,
            "HeartRateValue" : [],
            "HeartRateTime" : [],
            "CadenceValue" : [],
            "CadenceTime" : [],
            "LongitudePathValue" : [],
            "LatitudePathValue" : [],
            "AltitudePathValue" : [],
            "PathTime" : [],
            "SpeedValue" : [],
            "SpeedTime" : [],
            "PowerValue" : [],
            "PowerTime" : [],
            }
        record["Waypoints"] = waypoints;

        if activity.CountTotalWaypoints() > 1:
            flat_wps = activity.GetFlatWaypoints()

            anchor_ts = flat_wps[0].Timestamp

            # By default, use the provided waypoint types
            wp_type_iter = (wp.Type for wp in flat_wps)

            inPause = False
            for waypoint, waypoint_type in zip(flat_wps, wp_type_iter):
                timestamp = (waypoint.Timestamp - anchor_ts).total_seconds()

                if not inPause and waypoint_type == WaypointType.Pause:
                    inPause = True
                elif inPause and waypoint_type == WaypointType.Pause:
                    continue
                elif inPause and waypoint_type != WaypointType.Pause:
                    inPause = False

                if waypoint.HR is not None:
                    waypoints["HeartRateTime"].append(timestamp)
                    waypoints["HeartRateValue"].append(round(waypoint.HR))

                if waypoint.Power is not None:
                    waypoints["PowerTime"].append(timestamp)
                    waypoints["PowerValue"].append(waypoint.Power)

                if waypoint.Speed is not None:
                    waypoints["SpeedTime"].append(timestamp)
                    waypoints["SpeedValue"].append(waypoint.Speed)

                if waypoint.Cadence is not None:
                    waypoints["CadenceTime"].append(timestamp)
                    waypoints["CadenceValue"].append(waypoint.Cadence)

                if waypoint.Location is not None:
                    waypoints["PathTime"].append(timestamp)

                    if waypoint.Location.Longitude is not None and waypoint.Location.Latitude is not None:         
                        waypoints["LongitudePathValue"].append(waypoint.Location.Longitude)
                        waypoints["LatitudePathValue"].append(waypoint.Location.Latitude)
                    else:
                        waypoints["LongitudePathValue"].append(None)
                        waypoints["LatitudePathValue"].append(None)

                    waypoints["AltitudePathValue"].append(waypoint.Location.Altitude)

        return record

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
