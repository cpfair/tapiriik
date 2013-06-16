from tapiriik.settings import WEB_ROOT, AGGRESSIVE_CACHE
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException, APIExcludeActivity

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import json
import os
import logging

logger = logging.getLogger(__name__)

class StravaService(ServiceBase):
    ID = "strava"
    DisplayName = "Strava"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    UserProfileURL = "http://www.strava.com/athletes/{0}"
    UserActivityURL = "http://app.strava.com/activities/{1}"

    SupportedActivities = [ActivityType.Cycling]  # runs don't actually work with the API I'm using
    SupportsHR = True
    SupportsPower = True

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "strava"})

    def Authorize(self, email, password):
        # https://www.strava.com/api/v2/authentication/login
        params = {"email": email, "password": password}
        resp = requests.post("https://www.strava.com/api/v2/authentication/login", data=params)
        if resp.status_code != 200:
            raise APIAuthorizationException("Invalid login")
        data = resp.json()
        return (data["athlete"]["id"], {"Token": data["token"]})

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens strava distributes :\
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        # grumble grumble strava api sucks grumble grumble
        # http://app.strava.com/api/v1/rides?athleteId=id
        activities = []
        exclusions = []
        data = []
        offset = 0
        pgSz = 50  # this is determined by the Strava API
        earliestFirstPageDate = earliestDate = None
        while True:
            resp = requests.get("http://app.strava.com/api/v1/rides?offset=" + str(offset) + "&athleteId=" + str(svcRecord.ExternalID))
            reqdata = resp.json()
            reqdata = reqdata["rides"]
            data += reqdata
            if not exhaustive or len(reqdata) < pgSz:  # api returns 50 rows at a time, so once we start getting <50 we're done
                break
            offset += pgSz

        cachedRides = list(cachedb.strava_cache.find({"id": {"$in": [int(x["id"]) for x in data]}}))
        ct = 0
        for ride in data:
            cached = False
            if ride["id"] not in [x["id"] for x in cachedRides]:
                resp = requests.get("http://www.strava.com/api/v2/rides/" + str(ride["id"]))
                ridedata = resp.json()
                ridedata = ridedata["ride"]
                ridedata["Owner"] = svcRecord.ExternalID
            else:
                cached = True
                ridedata = [x for x in cachedRides if x["id"] == ride["id"]][0]
            if ridedata["start_latlng"] is None or ridedata["end_latlng"] is None or ridedata["distance"] is None or ridedata["distance"] == 0:
                exclusions.append(APIExcludeActivity("No path", activityId=ride["id"]))
                continue  # stationary activity - no syncing for now
            if ridedata["start_latlng"] == ridedata["end_latlng"]:
                exclusions.append(APIExcludeActivity("Only one waypoint", activityId=ride["id"]))
                continue  # Only one waypoint, one would assume.
            activity = UploadedActivity()
            activity.StartTime = datetime.strptime(ridedata["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
            activity.EndTime = activity.StartTime + timedelta(0, ridedata["elapsed_time"])
            activity.UploadedTo = [{"Connection": svcRecord, "ActivityID": ride["id"]}]
            activity.Type = ActivityType.Cycling  # change me once the API stops sucking
            activity.Distance = ridedata["distance"]
            activity.Name = ridedata["name"]
            activity.CalculateUID()
            activities.append(activity)
            if not earliestDate or activity.StartTime < earliestDate:
                earliestDate = activity.StartTime
            if ct == pgSz - 1:
                earliestFirstPageDate = earliestDate
            if not cached and (AGGRESSIVE_CACHE or ct < pgSz):  # Only cache the details we'll be needing immediately (on the 1st page of results)
                ridedata["StartTime"] = activity.StartTime
                cachedb.strava_cache.insert(ridedata)

            ct += 1
        if not AGGRESSIVE_CACHE:
            cachedb.strava_cache.remove({"Owner": svcRecord.ExternalID, "$or":[{"StartTime":{"$lt": earliestFirstPageDate}}, {"StartTime":{"$exists": False}}]})
        return activities, exclusions

    def DownloadActivity(self, svcRecord, activity):
        # thanks to Cosmo Catalano for the API reference code
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == svcRecord][0]

        if AGGRESSIVE_CACHE:
            ridedata = cachedb.strava_activity_cache.find_one({"id": activityID})
        if not AGGRESSIVE_CACHE or ridedata is None:
            resp = requests.get("http://app.strava.com/api/v1/streams/" + str(activityID), headers={"User-Agent": "Tapiriik-Sync"})
            ridedata = resp.json()
            ridedata["id"] = activityID
            ridedata["Owner"] = svcRecord.ExternalID
            if AGGRESSIVE_CACHE:
                cachedb.strava_activity_cache.insert(ridedata)

        activity.Waypoints = []

        hasHR = "heartrate" in ridedata and len(ridedata["heartrate"]) > 0
        hasCadence = "cadence" in ridedata and len(ridedata["cadence"]) > 0
        hasTemp = "temp" in ridedata and len(ridedata["temp"]) > 0
        hasPower = ("watts" in ridedata and len(ridedata["watts"]) > 0) or ("watts_calc" in ridedata and len(ridedata["watts_calc"]) > 0)
        hasAltitude = "altitude" in ridedata and len(ridedata["altitude"]) > 0
        hasRestingData = "resting" in ridedata and len(ridedata["resting"]) > 0
        moving = True

        if "error" in ridedata:
            raise APIException("Strava error " + ridedata["error"])

        hasLocation = False
        waypointCt = len(ridedata["time"])
        for idx in range(0, waypointCt - 1):
            latlng = ridedata["latlng"][idx]

            waypoint = Waypoint(activity.StartTime + timedelta(0, ridedata["time"][idx]))
            latlng = ridedata["latlng"][idx]
            waypoint.Location = Location(latlng[0], latlng[1], None)
            if waypoint.Location.Longitude == 0 and waypoint.Location.Latitude == 0:
                waypoint.Location.Longitude = None
                waypoint.Location.Latitude = None
            else:  # strava only returns 0 as invalid coords, so no need to check for null
                hasLocation = True
            if hasAltitude:
                waypoint.Location.Altitude = float(ridedata["altitude"][idx])

            if idx == 0:
                waypoint.Type = WaypointType.Start
            elif idx == waypointCt - 2:
                waypoint.Type = WaypointType.End
            elif hasRestingData and not moving and ridedata["resting"][idx] is False:
                waypoint.Type = WaypointType.Resume
                moving = True
            elif hasRestingData and ridedata["resting"][idx] is True:
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
        if not hasLocation:
            raise APIExcludeActivity("No waypoints with location", activityId=activityID)
        return activity

    def UploadActivity(self, serviceRecord, activity):
        # http://www.strava.com/api/v2/upload
        # POST token=asd&type=json&data_fields=[field1, field2, ...]&points=[[field1value, field2value, ...]...]&type=ride|run&name=name
        # hasHR = hasCadence = hasPower = False does Strava care?
        fields = ["time", "latitude", "longitude", "elevation", "cmd", "heartrate", "cadence", "watts"]
        points = []
        logger.info("activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))
        activity.EnsureTZ()
        for wp in activity.Waypoints:
            wpTime = wp.Timestamp - wp.Timestamp.utcoffset()  # strava y u do timezones wrong??
            points.append([wpTime.strftime("%Y-%m-%dT%H:%M:%S"),
                            wp.Location.Latitude if wp.Location is not None else "",
                            wp.Location.Longitude if wp.Location is not None else "",
                            wp.Location.Altitude if wp.Location is not None else "",
                            "pause" if wp.Type == WaypointType.Pause else None,
                            wp.HR,
                            wp.Cadence,
                            wp.Power
                            ])
        req = {"token": serviceRecord.Authorization["Token"],
                "type": "json",
                "id": "tap-sync-" + str(os.getpid()) + "-" + activity.UID + "-" + activity.UploadedTo[0]["Connection"].Service.ID,
                "data_fields": fields,
                "data": points,
                "activity_name": activity.Name,
                "activity_type": "run" if activity.Type == ActivityType.Running else "ride"}

        response = requests.post("http://www.strava.com/api/v2/upload", data=json.dumps(req), headers={"Content-Type": "application/json"})
        if response.status_code != 200:
            if response.status_code == 401:
                raise APIAuthorizationException("No authorization to upload activity " + activity.UID + " response " + response.text)
            raise APIException("Unable to upload activity " + activity.UID + " response " + response.text)

    def DeleteCachedData(self, serviceRecord):
        cachedb.strava_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.strava_activity_cache.remove({"Owner": serviceRecord.ExternalID})
