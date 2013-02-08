from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.database import db
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import httplib2
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
        wc = httplib2.Http()
        # https://www.strava.com/api/v2/authentication/login
        params = {"email": email, "password": password}
        resp, data = wc.request("https://www.strava.com/api/v2/authentication/login", method="POST", body=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if resp.status != 200:
            return (None, None)  # maybe raise an exception instead?
        data = json.loads(data.decode('utf-8'))
        return (data["athlete"]["id"], {"Token": data["token"]})

    def DownloadActivityList(self, svcRecord):
        wc = httplib2.Http()
        # grumble grumble strava api sucks grumble grumble
        # http://app.strava.com/api/v1/rides?athleteId=id
        activities = []
        resp, data = wc.request("http://app.strava.com/api/v1/rides?athleteId=" + str(svcRecord["ExternalID"]))
        data = json.loads(data.decode('utf-8'))


        data = data["rides"]
        cachedRides = list(db.strava_cache.find({"id": {"$in": [int(x["id"]) for x in data]}}))
        for ride in data:
            if ride["id"] not in [x["id"] for x in cachedRides]:
                resp, ridedata = wc.request("http://www.strava.com/api/v2/rides/" + str(ride["id"]))
                ridedata = json.loads(ridedata.decode('utf-8'))
                ridedata = ridedata["ride"]
                db.strava_cache.insert(ridedata)
            else:
                ridedata = [x for x in cachedRides if x["id"] == ride["id"]][0]
            activity = UploadedActivity()
            activity.StartTime = datetime.strptime(ridedata["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
            activity.EndTime = activity.StartTime + timedelta(0, ridedata["elapsed_time"])
            activity.UploadedTo = [{"Connection": svcRecord, "ActivityID": ride["id"]}]
            activity.CalculateUID()
            activities.append(activity)

        return activities

    def DownloadActivity(self, svcRecord, activity):
        # thanks to Cosmo Catalano for the API reference code
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == svcRecord][0]

        ridedata = db.strava_activity_cache.find_one({"id": activityID})
        if ridedata is None:
            wc = httplib2.Http()
            resp, ridedata = wc.request("http://app.strava.com/api/v1/streams/" + str(activityID), headers={"User-Agent": "Tapiriik-Sync"})
            ridedata = json.loads(ridedata.decode('utf-8'))
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
            elif idx == waypointCt - 1:
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
