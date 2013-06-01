from tapiriik.settings import WEB_ROOT, RUNKEEPER_CLIENT_ID, RUNKEEPER_CLIENT_SECRET, AGGRESSIVE_CACHE
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, APIAuthorizationException
from tapiriik.services.interchange import UploadedActivity, ActivityType, WaypointType, Waypoint, Location
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import urllib.parse
import json


class RunKeeperService(ServiceBase):
    ID = "runkeeper"
    DisplayName = "RunKeeper"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://runkeeper.com/user/{0}/profile"

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
            raise APIAuthorizationException("Invalid code")
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
        if resp.status_code != 204:
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
                    raise APIAuthorizationException("No authorization to retrieve user URLs")
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
                    raise APIAuthorizationException("No authorization to retrieve activity list")
                raise APIException("Unable to retrieve activity list " + str(response) + " " + response.text)
            data = response.json()
            allItems += data["items"]
            if not exhaustive or "next" not in data or data["next"] == "":
                break
            pageUri = "https://api.runkeeper.com" + data["next"]

        activities = []
        for act in allItems:
            if "has_path" in act and act["has_path"] is False:
                continue  # No points = no sync.
            if "is_live" in act and act["is_live"] is True:
                continue  # Otherwise we end up with partial activities.
            activity = self._populateActivity(act)
            if (activity.StartTime - activity.EndTime).total_seconds() == 0:
                continue  # these activites are corrupted
            activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["uri"]}]
            activities.append(activity)
        return activities

    def _populateActivity(self, rawRecord):
        ''' Populate the 1st level of the activity object with all details required for UID from RK API data '''
        activity = UploadedActivity()
        #  can stay local + naive here, recipient services can calculate TZ as required
        activity.StartTime = datetime.strptime(rawRecord["start_time"], "%a, %d %b %Y %H:%M:%S")
        activity.EndTime = activity.StartTime + timedelta(0, round(rawRecord["duration"]))  # this is inaccurate with pauses - excluded from hash
        activity.Distance = rawRecord["total_distance"]
        if rawRecord["type"] in self._activityMappings:
            activity.Type = self._activityMappings[rawRecord["type"]]

        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        if AGGRESSIVE_CACHE:
            ridedata = cachedb.rk_activity_cache.find_one({"uri": activityID})
        if not AGGRESSIVE_CACHE or ridedata is None:
            response = requests.get("https://api.runkeeper.com" + activityID, headers=self._apiHeaders(serviceRecord))
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIAuthorizationException("No authorization to download activity" + activityID)
                raise APIException("Unable to download activity " + activityID + " response " + str(response) + " " + response.text)
            ridedata = response.json()
            ridedata["Owner"] = serviceRecord.ExternalID
            if AGGRESSIVE_CACHE:
                cachedb.rk_activity_cache.insert(ridedata)

        self._populateActivityWaypoints(ridedata, activity)

        if len(activity.Waypoints) <= 1:
            activity.Exclude = True

        return activity

    def _populateActivityWaypoints(self, rawData, activity):
        ''' populate the Waypoints collection from RK API data '''
        activity.Waypoints = []

        #  path is the primary stream, HR/power/etc must have fewer pts
        hasHR = "heart_rate" in rawData and len(rawData["heart_rate"]) > 0
        hasCalories = "calories" in rawData and len(rawData["calories"]) > 0
        for pathpoint in rawData["path"]:
            waypoint = Waypoint(activity.StartTime + timedelta(0, pathpoint["timestamp"]))
            waypoint.Location = Location(pathpoint["latitude"], pathpoint["longitude"], pathpoint["altitude"] if "altitude" in pathpoint and float(pathpoint["altitude"]) != 0 else None)  # if you're running near sea level, well...
            waypoint.Type = self._wayptTypeMappings[pathpoint["type"]] if pathpoint["type"] in self._wayptTypeMappings else WaypointType.Regular

            if hasHR:
                hrpoint = [x for x in rawData["heart_rate"] if x["timestamp"] == pathpoint["timestamp"]]
                if len(hrpoint) > 0:
                    waypoint.HR = hrpoint[0]["heart_rate"]
            if hasCalories:
                calpoint = [x for x in rawData["calories"] if x["timestamp"] == pathpoint["timestamp"]]
                if len(calpoint) > 0:
                    waypoint.Calories = calpoint[0]["calories"]

            activity.Waypoints.append(waypoint)

    def UploadActivity(self, serviceRecord, activity):
        #  assembly dict to post to RK
        uploadData = self._createUploadData(activity)
        uris = self._getAPIUris(serviceRecord)
        headers = self._apiHeaders(serviceRecord)
        headers["Content-Type"] = "application/vnd.com.runkeeper.NewFitnessActivity+json"
        response = requests.post(uris["fitness_activities"], headers=headers, data=json.dumps(uploadData))

        if response.status_code != 201:
            if response.status_code == 401 or response.status_code == 403:
                raise APIAuthorizationException("No authorization to upload activity " + activity.UID)
            raise APIException("Unable to upload activity " + activity.UID + " response " + str(response) + " " + response.text)

    def _createUploadData(self, activity):
        ''' create data dict for posting to RK API '''
        record = {}

        record["type"] = [key for key in self._activityMappings if self._activityMappings[key] == activity.Type][0]
        record["start_time"] = activity.StartTime.strftime("%a, %d %b %Y %H:%M:%S")
        record["duration"] = (activity.EndTime - activity.StartTime).total_seconds()
        if activity.Distance is not None:
            record["total_distance"] = activity.Distance  # RK calculates this itself, so we probably don't care
        record["notes"] = activity.Name  # not symetric, but better than nothing
        record["path"] = []
        for waypoint in activity.Waypoints:
            timestamp = (waypoint.Timestamp - activity.StartTime).total_seconds()

            if waypoint.Type in self._wayptTypeMappings.values():
                wpType = [key for key, value in self._wayptTypeMappings.items() if value == waypoint.Type][0]
            else:
                wpType = "gps"  # meh

            if waypoint.Location is None or waypoint.Location.Latitude is None or waypoint.Location.Longitude is None:
                continue

            if waypoint.Location is not None and waypoint.Location.Latitude is not None and waypoint.Location.Longitude is not None:
                pathPt = {"timestamp": timestamp,
                          "latitude": waypoint.Location.Latitude,
                          "longitude": waypoint.Location.Longitude,
                          "type": wpType}
                pathPt["altitude"] = waypoint.Location.Altitude if waypoint.Location.Altitude is not None else 0  # this is straight of of their "example calls" page
                record["path"].append(pathPt)

            if waypoint.HR is not None:
                if "heart_rate" not in record:
                    record["heart_rate"] = []
                record["heart_rate"].append({"timestamp": timestamp, "heart_rate": waypoint.HR})

            if waypoint.Calories is not None:
                if "calories" not in record:
                    record["calories"] = []
                record["calories"].append({"timestamp": timestamp, "calories": waypoint.Calories})

        return record

    def DeleteCachedData(self, serviceRecord):
        cachedb.rk_activity_cache.remove({"Owner": serviceRecord.ExternalID})
