from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.database import db
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import urllib.parse
import json


class StravaService:
    ID = "strava"
    DisplayName = "Strava"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword

    SupportedActivities = [ActivityType.Running, ActivityType.Cycling]
    SupportsHR = True
    SupportsPower = True
    SupportsCalories = False  # don't think it does

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "strava"})

    def Authorize(self, email, password):
        # https://www.strava.com/api/v2/authentication/login
        params = {"email": email, "password": password}
        resp = requests.post("https://www.strava.com/api/v2/authentication/login", data=params)
        if resp.status_code != 200:
            return (None, None)  # maybe raise an exception instead?
        data = resp.json()
        return (data["athlete"]["id"], {"Token": data["token"]})

    def RevokeAuthorization(self, serviceRecord):
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        if exhaustive:
            raise NotImplementedError
        # grumble grumble strava api sucks grumble grumble
        # http://app.strava.com/api/v1/rides?athleteId=id
        activities = []
        resp = requests.get("http://app.strava.com/api/v1/rides?athleteId=" + str(svcRecord["ExternalID"]))
        data = resp.json()

        data = data["rides"]
        cachedRides = list(db.strava_cache.find({"id": {"$in": [int(x["id"]) for x in data]}}))
        for ride in data:
            if ride["id"] not in [x["id"] for x in cachedRides]:
                resp = requests.get("http://www.strava.com/api/v2/rides/" + str(ride["id"]))
                ridedata = resp.json()
                ridedata = ridedata["ride"]
                db.strava_cache.insert(ridedata)
            else:
                ridedata = [x for x in cachedRides if x["id"] == ride["id"]][0]
            activity = UploadedActivity()
            activity.StartTime = datetime.strptime(ridedata["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
            activity.EndTime = activity.StartTime + timedelta(0, ridedata["elapsed_time"])
            activity.UploadedTo = [{"Connection": svcRecord, "ActivityID": ride["id"]}]
            activity.Type = ActivityType.Cycling  # change me once the API stops sucking
            activity.CalculateUID()
            activities.append(activity)

        return activities

    def DownloadActivity(self, svcRecord, activity):
        # thanks to Cosmo Catalano for the API reference code
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == svcRecord][0]

        ridedata = db.strava_activity_cache.find_one({"id": activityID})
        if ridedata is None:
            resp = requests.get("http://app.strava.com/api/v1/streams/" + str(activityID), headers={"User-Agent": "Tapiriik-Sync"})
            ridedata = resp.json()
            ridedata["id"] = activityID
            db.strava_activity_cache.insert(ridedata)

        activity.Waypoints = []

        hasHR = "heartrate" in ridedata and len(ridedata["heartrate"]) > 0
        hasCadence = "cadence" in ridedata and len(ridedata["cadence"]) > 0
        hasTemp = "temp" in ridedata and len(ridedata["tmep"]) > 0
        hasPower = ("watts" in ridedata and len(ridedata["watts"]) > 0) or ("watts_calc" in ridedata and len(ridedata["watts_calc"]) > 0)
        moving = True
        waypointCt = len(ridedata["time"])
        for idx in range(0, waypointCt - 1):
            latlng = ridedata["latlng"][idx]

            waypoint = Waypoint(activity.StartTime + timedelta(0, ridedata["time"][idx]))
            latlng = ridedata["latlng"][idx]
            waypoint.Location = Location(latlng[0], latlng[1], ridedata["altitude"][idx])

            if idx == 0:
                waypoint.Type = WaypointType.Start
            elif idx == waypointCt - 2:
                waypoint.Type = WaypointType.End
            elif not moving and ridedata["moving"][idx] == True:
                waypoint.Type = WaypointType.Resume
                moving = True
            elif ridedata["moving"][idx] == False:
                waypoint.Type = WaypointType.Pause
                moving = False

            if hasHR:
                waypoint.HR = ridedata["heartrate"][idx]
            if hasCadence:
                waypoint.Cadence = ridedata["cadence"][idx]
            if hasTemp:
                waypoint.Temp = ridedata["temp"][idx]
            if hasPower:
                waypoint.Power = ridedata["watts"][idx] if "watts" in ridedata else ridedata["watts_calc"][idx]
            activity.Waypoints.append(waypoint)

        return activity

    def UploadActivity(self, serviceRecord, activity):
        # http://www.strava.com/api/v2/upload
        # POST token=asd&type=json&data_fields=[field1, field2, ...]&points=[[field1value, field2value, ...]...]&type=ride|run&name=name
        # hasHR = hasCadence = hasPower = False does Strava care?
        fields = ["time", "latitude", "longitude", "elevation", "cmd", "heartrate", "cadence", "watts"]
        points = []
        for wp in activity.Waypoints:
            points.append([wp.Timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                            wp.Location.Latitude,
                            wp.Location.Longitude,
                            wp.Location.Altitude,
                            "pause" if wp.Type == WaypointType.Pause else None,
                            wp.HR,
                            wp.Cadence,
                            wp.Power
                            ])
        req = {"token": serviceRecord["Authorization"]["Token"],
                "type": "json",
                "data_fields": fields,
                "data": points,
                "activity_type": "run" if activity.Type == ActivityType.Running else "ride"}

        response = requests.post("http://www.strava.com/api/v2/upload", data=json.dumps(req), proxies={"http": "127.0.0.1:8181"})
        if response.status_code != 200:
            if response.status_code == 401:
                raise APIAuthorizationException("No authorization to upload activity " + activity.UID + " response " + response.text, serviceRecord)
            raise APIException("Unable to upload activity " + activity.UID + " response " + response.text, serviceRecord)


    def DeleteCachedData(self, serviceRecord):
        pass
