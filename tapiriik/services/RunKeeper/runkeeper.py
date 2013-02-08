from tapiriik.settings import WEB_ROOT, RUNKEEPER_CLIENT_ID, RUNKEEPER_CLIENT_SECRET
from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.services.interchange import UploadedActivity, ActivityType, WaypointType, Waypoint, Location
from tapiriik.database import db
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import httplib2
import urllib.parse
import json


class RunKeeperService():
    ID = "runkeeper"
    DisplayName = "RunKeeper"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserAuthorizationURL = None

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
    SupportsCadence = False
    SupportsTemp = False
    SupportsPower = False

    _wayptTypeMappings = {"start": WaypointType.Start, "end": WaypointType.End, "pause": WaypointType.Pause, "resume": WaypointType.Resume}

    def WebInit(self):
        self.UserAuthorizationURL = "https://runkeeper.com/apps/authorize?client_id=" + RUNKEEPER_CLIENT_ID + "&response_type=code&redirect_uri=" + WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})

    def RetrieveAuthenticationToken(self, req):
        from tapiriik.services import Service

        wc = httplib2.Http()
        #  might consider a real OAuth client
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": RUNKEEPER_CLIENT_ID, "client_secret": RUNKEEPER_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})}
        #return urllib.parse.urlencode(params)
        resp, data = wc.request("https://runkeeper.com/apps/token", method="POST", body=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if resp.status != 200:
            raise ValueError("Invalid code")
        token = json.loads(data.decode('utf-8'))["access_token"]

        # hacky, but also totally their fault for not giving the user id in the token req
        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Token": token})
        if existingRecord is None:
            uid = self._getUserId({"Authorization": {"Token": token}})  # meh
        else:
            uid = existingRecord["ExternalID"]

        return (uid, {"Token": token})

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "Bearer " + serviceRecord["Authorization"]["Token"]}

    def _getAPIUris(self, serviceRecord):
        if hasattr(self, "_uris"):  # cache these for the life of the batch job at least
            return self._uris
        else:
            wc = httplib2.Http()
            resp, data = wc.request("https://api.runkeeper.com/user/", headers=self._apiHeaders(serviceRecord))
            uris = json.loads(data.decode('utf-8'))
            for k in uris.keys():
                if type(uris[k]) == str:
                    uris[k] = "https://api.runkeeper.com" + uris[k]
            self._uris = uris
            return uris

    def _getUserId(self, serviceRecord):
        wc = httplib2.Http()
        resp, data = wc.request("https://api.runkeeper.com/user/", headers=self._apiHeaders(serviceRecord))
        data = json.loads(data.decode('utf-8'))
        return data["userID"]

    def DownloadActivityList(self, serviceRecord):
        uris = self._getAPIUris(serviceRecord)
        wc = httplib2.Http()
        resp, data = wc.request(uris["fitness_activities"], headers=self._apiHeaders(serviceRecord))
        data = json.loads(data.decode('utf-8'))
        activities = []
        for act in data["items"]:
            activity = self._populateActivity(act)
            activity.UploadedTo = [{"Connection":serviceRecord, "ActivityID":act["uri"]}]
            activities.append(activity)
        return activities

    def _populateActivity(self, rawRecord):
        ''' Populate the 1st level of the activity object with all details required for UID from RK API data '''
        activity = UploadedActivity()
        activity.StartTime = datetime.strptime(rawRecord["start_time"], "%a, %d %b %Y %H:%M:%S")
        activity.EndTime = activity.StartTime + timedelta(0, round(rawRecord["duration"]))  # this is inaccurate with pauses - excluded from hash
        if rawRecord["type"] in self._activityMappings:
            activity.Type = self._activityMappings[rawRecord["type"]]

        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        ridedata = db.rk_activity_cache.find_one({"uri": activityID})
        if ridedata is None:
            wc = httplib2.Http()
            resp, ridedata = wc.request("https://api.runkeeper.com" + activityID, headers=self._apiHeaders(serviceRecord))
            ridedata = json.loads(ridedata.decode('utf-8'))
            db.rk_activity_cache.insert(ridedata)

        self._populateActivityWaypoints(ridedata, activity)

        return activity

    def _populateActivityWaypoints(self, rawData, activity):
        ''' populate the Waypoints collection from RK API data '''
        activity.Waypoints = []

        #  path is the primary stream, HR/power/etc must have fewer pts
        hasHR = "heart_rate" in rawData and len(rawData["heart_rate"]) > 0
        hasCalories = "calories" in rawData and len(rawData["calories"]) > 0
        for pathpoint in rawData["path"]:
            waypoint = Waypoint(activity.StartTime + timedelta(0, pathpoint["timestamp"]))
            waypoint.Location = Location(pathpoint["latitude"], pathpoint["longitude"], pathpoint["altitude"])
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
        pass

    def _createUploadData(self, activity):
        ''' create data dict for posting to RK API '''
        record = {}

        record["type"] = [key for key in self._activityMappings if self._activityMappings[key] == activity.Type][0]
        record["start_time"] = activity.StartTime.strftime("%a, %d %b %Y %H:%M:%S")
        record["duration"] = (activity.EndTime - activity.StartTime).total_seconds()

        record["path"] = []
        record["heart_rate"] = []
        record["calories"] = []
        for waypoint in activity.Waypoints:
            timestamp = (waypoint.Timestamp - activity.StartTime).total_seconds()
            
            
            if waypoint.Type in self._wayptTypeMappings.values():
                wpType = [key for key, value in self._wayptTypeMappings.items() if value == waypoint.Type][0]
            else:
                wpType = "gps"  # meh

            record["path"].append({"timestamp": timestamp,
                                    "latitude": waypoint.Location.Latitude,
                                    "longitude": waypoint.Location.Longitude,
                                    "altitude": waypoint.Location.Altitude,
                                    "type": wpType})

            if waypoint.HR is not None:
                record["heart_rate"].append({"timestamp": timestamp, "heart_rate": waypoint.HR})

            if waypoint.Calories is not None:
                record["calories"].append({"timestamp": timestamp, "calories": waypoint.Calories})

        return record
