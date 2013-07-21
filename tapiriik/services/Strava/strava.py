from tapiriik.settings import WEB_ROOT, AGGRESSIVE_CACHE, STRAVA_CLIENT_SECRET, STRAVA_CLIENT_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException, APIExcludeActivity

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import calendar
import requests
import json
import os
import logging
import pytz
import re

logger = logging.getLogger(__name__)

class StravaService(ServiceBase):
    ID = "strava"
    DisplayName = "Strava"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    UserProfileURL = "http://www.strava.com/athletes/{0}"
    UserActivityURL = "http://app.strava.com/activities/{1}"

    SupportedActivities = [ActivityType.Cycling, ActivityType.Running, ActivityType.MountainBiking]
    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    _activityTypeMappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.MountainBiking: "Ride",
        ActivityType.Running: "Run"
    }

    _reverseActivityTypeMappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.Running: "Run"
    }

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "strava"})

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def Authorize(self, email, password):
        # https://www.strava.com/api/v3/oauth/internal/token
        params = {"email": email, "password": password, "client_secret": STRAVA_CLIENT_SECRET, "client_id": STRAVA_CLIENT_ID}
        resp = requests.post("https://www.strava.com/api/v3/oauth/internal/token", data=params)
        if resp.status_code != 200:
            raise APIAuthorizationException("Invalid login")
        data = resp.json()

        authorizationData = {"OAuthToken": data["access_token"]}
        # Retrieve the user ID, meh.
        id_resp = requests.get("https://www.strava.com/api/v3/athlete", headers=self._apiHeaders(ServiceRecord({"Authorization": authorizationData})))

        return (id_resp.json()["id"], authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens strava distributes :\
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        # grumble grumble strava api sucks grumble grumble
        # http://app.strava.com/api/v1/rides?athleteId=id
        activities = []
        exclusions = []
        before = earliestDate = None

        while True:
            resp = requests.get("https://www.strava.com/api/v3/athletes/" + str(svcRecord.ExternalID) + "/activities", headers=self._apiHeaders(svcRecord), params={"before": before})
            logger.debug("Req with before=" + str(before) + "/" + str(earliestDate))

            earliestDate = None

            reqdata = resp.json()

            if not len(reqdata):
                break  # No more activities to see

            for ride in reqdata:
                activity = UploadedActivity()
                activity.TZ = pytz.timezone(re.sub("^\([^\)]+\)\s*", "", ride["timezone"]))  # Comes back as "(GMT -13:37) The Stuff/We Want""
                activity.StartTime = pytz.utc.localize(datetime.strptime(ride["start_date"], "%Y-%m-%dT%H:%M:%SZ"))
                logger.debug("\tActivity s/t " + str(activity.StartTime))
                if not earliestDate or activity.StartTime < earliestDate:
                    earliestDate = activity.StartTime
                    before = calendar.timegm(activity.StartTime.astimezone(pytz.utc).timetuple())

                if ride["start_latlng"] is None or ride["end_latlng"] is None or ride["distance"] is None or ride["distance"] == 0:
                    exclusions.append(APIExcludeActivity("No path", activityId=ride["id"]))
                    continue  # stationary activity - no syncing for now
                if ride["start_latlng"] == ride["end_latlng"]:
                    exclusions.append(APIExcludeActivity("Only one waypoint", activityId=ride["id"]))
                    continue  # Only one waypoint, one would assume.


                activity.EndTime = activity.StartTime + timedelta(0, ride["elapsed_time"])
                activity.UploadedTo = [{"Connection": svcRecord, "ActivityID": ride["id"]}]

                actType = [k for k, v in self._reverseActivityTypeMappings.items() if v == ride["type"]]
                if not len(actType):
                    exclusions.append(APIExcludeActivity("Unsupported activity type", activityId=ride["id"]))
                    continue

                activity.Type = actType[0]
                activity.Distance = ride["distance"]
                activity.Name = ride["name"]
                activity.AdjustTZ()
                activity.CalculateUID()
                activities.append(activity)

            if not exhaustive or not earliestDate:
                break

        return activities, exclusions

    def DownloadActivity(self, svcRecord, activity):
        # thanks to Cosmo Catalano for the API reference code
        activityID = [x["ActivityID"] for x in activity.UploadedTo if x["Connection"] == svcRecord][0]

        streamdata = requests.get("https://www.strava.com/api/v3/activities/" + str(activityID) + "/streams/time,altitude,heartrate,cadence,watts,watts_calc,temp,resting,latlng", headers=self._apiHeaders(svcRecord))
        streamdata = streamdata.json()

        if "message" in streamdata and streamdata["message"] == "Record Not Found":
            raise APIException("Could not find activity")

        ridedata = {}
        for stream in streamdata:
            ridedata[stream["type"]] = stream["data"]

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
            else:  # strava only returns 0 as invalid coords, so no need to check for null (update: ??)
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
        fields = ["time", "latlng", "elevation", "cmd", "heartrate", "cadence", "watts", "temp"]
        points = []
        logger.info("Activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))
        activity.EnsureTZ()
        for wp in activity.Waypoints:
            wpTime = wp.Timestamp.astimezone(pytz.utc)
            points.append([calendar.timegm(wpTime.timetuple()),
                            [wp.Location.Latitude if wp.Location is not None else "", wp.Location.Longitude if wp.Location is not None else ""],
                            wp.Location.Altitude if wp.Location is not None else "",
                            "pause" if wp.Type == WaypointType.Pause else None,
                            wp.HR,
                            wp.Cadence,
                            wp.Power,
                            wp.Temp
                            ])
        logger.info("First wp unix ts " + str(points[0][0]))
        req = { "id": 0,
                "activity_id": 0,
                "sample_rate": 0,
                "start_date": points[0][0],
                "data_type": "json",
                "external_id": "tap-sync-" + str(os.getpid()) + "-" + activity.UID + "-" + activity.UploadedTo[0]["Connection"].Service.ID,
                "data": json.dumps([{"fields": fields, "values": points}]),
                "activity_name": activity.Name,
                "activity_type": self._activityTypeMappings[activity.Type],
                "time_series_field": "time"}

        response = requests.post("http://www.strava.com/api/v3/uploads", data=req, headers=self._apiHeaders(serviceRecord)) #{"Content-Type": "application/json"}
        if response.status_code != 200:
            if response.status_code == 401:
                raise APIAuthorizationException("No authorization to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))
            raise APIException("Unable to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))

    def DeleteCachedData(self, serviceRecord):
        cachedb.strava_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.strava_activity_cache.remove({"Owner": serviceRecord.ExternalID})
