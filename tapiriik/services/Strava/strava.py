from tapiriik.settings import WEB_ROOT, STRAVA_CLIENT_SECRET, STRAVA_CLIENT_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import calendar
import requests
import os
import logging
import pytz
import re
import time

logger = logging.getLogger(__name__)

class StravaService(ServiceBase):
    ID = "strava"
    DisplayName = "Strava"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://www.strava.com/athletes/{0}"
    UserActivityURL = "http://app.strava.com/activities/{1}"
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    # For mapping Strava->common; no ambiguity in Strava activity type
    _activityTypeMappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.MountainBiking: "Ride",
        ActivityType.Hiking: "Hike",
        ActivityType.Running: "Run",
        ActivityType.Walking: "Walk",
        ActivityType.Snowboarding: "Snowboard",
        ActivityType.Skating: "IceSkate",
        ActivityType.CrossCountrySkiing: "BackcountrySki",
        ActivityType.DownhillSkiing: "NordicSki",
        ActivityType.DownhillSkiing: "AlpineSki",
        ActivityType.Swimming: "Swim"
    }

    # For mapping common->Strava
    _reverseActivityTypeMappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.MountainBiking: "MountainBiking",
        ActivityType.Running: "Run",
        ActivityType.Hiking: "Hike",
        ActivityType.Walking: "Walk",
        ActivityType.DownhillSkiing: "AlpineSki",
        ActivityType.CrossCountrySkiing: "BackcountrySki",
        ActivityType.Swimming: "Swim",
        ActivityType.Skating: "IceSkate"
    }

    SupportedActivities = list(_reverseActivityTypeMappings.keys())

    def WebInit(self):
        self.UserAuthorizationURL = "https://www.strava.com/oauth/authorize?scope=write%20view_private&client_id=" + STRAVA_CLIENT_ID + "&response_type=code&redirect_uri=http://tapiriik.com"  + reverse("oauth_return", kwargs={"service": "strava"})

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "runkeeper"})}

        response = requests.post("https://www.strava.com/oauth/token", data=params)
        if response.status_code != 200:
            raise APIException("Invalid code")
        data = response.json()

        authorizationData = {"OAuthToken": data["access_token"]}
        # Retrieve the user ID, meh.
        id_resp = requests.get("https://www.strava.com/api/v3/athlete", headers=self._apiHeaders(ServiceRecord({"Authorization": authorizationData})))

        return (id_resp.json()["id"], authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens strava distributes :\
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        activities = []
        exclusions = []
        before = earliestDate = None

        while True:
            logger.debug("Req with before=" + str(before) + "/" + str(earliestDate))
            resp = requests.get("https://www.strava.com/api/v3/athletes/" + str(svcRecord.ExternalID) + "/activities", headers=self._apiHeaders(svcRecord), params={"before": before})
            if resp.status_code == 401:
                raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

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
                    logger.debug("\t\tNo pts")
                    continue  # stationary activity - no syncing for now


                activity.EndTime = activity.StartTime + timedelta(0, ride["elapsed_time"])
                activity.UploadedTo = [{"Connection": svcRecord, "ActivityID": ride["id"]}]

                actType = [k for k, v in self._reverseActivityTypeMappings.items() if v == ride["type"]]
                if not len(actType):
                    exclusions.append(APIExcludeActivity("Unsupported activity type %s" % ride["type"], activityId=ride["id"]))
                    logger.debug("\t\tUnknown activity")
                    continue

                activity.Type = actType[0]
                activity.Distance = ride["distance"]
                activity.Name = ride["name"]
                activity.Private = ride["private"]
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
        if streamdata.status_code == 401:
            raise APIException("No authorization to download activity", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

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
        hasPower = ("watts" in ridedata and len(ridedata["watts"]) > 0)
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
                waypoint.Power = ridedata["watts"][idx]
            activity.Waypoints.append(waypoint)
        if not hasLocation:
            raise APIExcludeActivity("No waypoints with location", activityId=activityID)
        return activity

    def UploadActivity(self, serviceRecord, activity):
        logger.info("Activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))

        req = { "id": 0,
                "data_type": "tcx",
                "external_id": "tap-sync-" + str(os.getpid()) + "-" + activity.UID + "-" + activity.UploadedTo[0]["Connection"].Service.ID,
                "activity_name": activity.Name,
                "activity_type": self._activityTypeMappings[activity.Type],
                "private": activity.Private}

        if "tcx" in activity.PrerenderedFormats:
            logger.debug("Using prerendered TCX")
            tcxData = activity.PrerenderedFormats["tcx"]
        else:
            activity.EnsureTZ()
            tcxData = TCXIO.Dump(activity)
        # TODO: put the tcx back into PrerenderedFormats once there's more RAM to go around and there's a possibility of it actually being used.
        files = {"file":(req["external_id"] + ".tcx", tcxData)}

        response = requests.post("http://www.strava.com/api/v3/uploads", data=req, files=files, headers=self._apiHeaders(serviceRecord))
        if response.status_code != 201:
            if response.status_code == 401:
                raise APIException("No authorization to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Unable to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))


        upload_id = response.json()["id"]
        while not response.json()["activity_id"]:
            time.sleep(1)
            response = requests.get("http://www.strava.com/api/v3/uploads/%s" % upload_id, headers=self._apiHeaders(serviceRecord))
            logger.debug("Waiting for upload - status %s id %s" % (response.json()["status"], response.json()["activity_id"]))
            if response.json()["error"]:
                error = response.json()["error"]
                if "duplicate of activity" in error:
                    logger.debug("Duplicate")
                    return # I guess we're done here?
                raise APIException("Strava failed while processing activity - last status %s" % response.text)

    def DeleteCachedData(self, serviceRecord):
        cachedb.strava_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.strava_activity_cache.remove({"Owner": serviceRecord.ExternalID})
