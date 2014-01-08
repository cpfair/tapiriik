from tapiriik.settings import WEB_ROOT, STRAVA_CLIENT_SECRET, STRAVA_CLIENT_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.fit import FITIO

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
    LastUpload = None

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    # For mapping common->Strava; no ambiguity in Strava activity type
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
        ActivityType.Swimming: "Swim",
        ActivityType.Gym: "Workout"
    }

    # For mapping Strava->common
    _reverseActivityTypeMappings = {
        "Ride": ActivityType.Cycling,
        "MountainBiking": ActivityType.MountainBiking,
        "Run": ActivityType.Running,
        "Hike": ActivityType.Hiking,
        "Walk": ActivityType.Walking,
        "AlpineSki": ActivityType.DownhillSkiing,
        "NordicSki": ActivityType.DownhillSkiing,
        "BackcountrySki": ActivityType.CrossCountrySkiing,
        "Swim": ActivityType.Swimming,
        "IceSkate": ActivityType.Skating,
        "Workout": ActivityType.Gym
    }

    SupportedActivities = list(_activityTypeMappings.keys())

    def WebInit(self):
        self.UserAuthorizationURL = "https://www.strava.com/oauth/authorize?scope=write%20view_private&client_id=" + STRAVA_CLIENT_ID + "&response_type=code&redirect_uri=http://tapiriik.com"  + reverse("oauth_return", kwargs={"service": "strava"})

    def _logAPICall(self, endpoint, opkey, error):
        cachedb.strava_apicall_stats.insert({"Endpoint": endpoint, "Opkey": opkey, "Error": error, "Timestamp": datetime.utcnow()})

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "strava"})}

        response = requests.post("https://www.strava.com/oauth/token", data=params)
        self._logAPICall("auth-token", None, response.status_code != 200)
        if response.status_code != 200:
            raise APIException("Invalid code")
        data = response.json()

        authorizationData = {"OAuthToken": data["access_token"]}
        # Retrieve the user ID, meh.
        id_resp = requests.get("https://www.strava.com/api/v3/athlete", headers=self._apiHeaders(ServiceRecord({"Authorization": authorizationData})))
        self._logAPICall("auth-extid", None, None)
        return (id_resp.json()["id"], authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens strava distributes :\
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        activities = []
        exclusions = []
        before = earliestDate = None

        while True:
            if before is not None and before < 0:
                break # Caused by activities that "happened" before the epoch. We generally don't care about those activities...
            logger.debug("Req with before=" + str(before) + "/" + str(earliestDate))
            resp = requests.get("https://www.strava.com/api/v3/athletes/" + str(svcRecord.ExternalID) + "/activities", headers=self._apiHeaders(svcRecord), params={"before": before})
            self._logAPICall("list", (svcRecord.ExternalID, str(earliestDate)), resp.status_code == 401)
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

                manual = False  # Determines if we bother to "download" the activity afterwards
                if ride["start_latlng"] is None or ride["end_latlng"] is None:
                    manual = True

                activity.EndTime = activity.StartTime + timedelta(0, ride["elapsed_time"])
                activity.ServiceData = {"ActivityID": ride["id"], "Manual": manual}

                if ride["type"] not in self._reverseActivityTypeMappings:
                    exclusions.append(APIExcludeActivity("Unsupported activity type %s" % ride["type"], activityId=ride["id"]))
                    logger.debug("\t\tUnknown activity")
                    continue

                activity.Type = self._reverseActivityTypeMappings[ride["type"]]
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=ride["distance"])
                if "max_speed" in ride or "average_speed" in ride:
                    activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=ride["average_speed"] if "average_speed" in ride else None, max=ride["max_speed"] if "max_speed" in ride else None)
                activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=ride["moving_time"]) if "moving_time" in ride and ride["moving_time"] > 0 else None)  # They don't let you manually enter this, and I think it returns 0 for those activities.
                # Strava doesn't handle "timer time" to the best of my knowledge - although they say they do look at the FIT total_timer_time field, so...?
                if "average_watts" in ride:
                    activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=ride["average_watts"])
                if "average_heartrate" in ride:
                    activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=ride["average_heartrate"]))
                if "max_heartrate" in ride:
                    activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=ride["max_heartrate"]))
                if "average_cadence" in ride:
                    activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=ride["average_cadence"]))
                if "average_temp" in ride:
                    activity.Stats.Temperature.update(ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, avg=ride["average_temp"]))
                if "calories" in ride:
                    activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=ride["calories"])
                activity.Name = ride["name"]
                activity.Private = ride["private"]
                activity.Stationary = manual
                activity.AdjustTZ()
                activity.CalculateUID()
                activities.append(activity)

            if not exhaustive or not earliestDate:
                break

        return activities, exclusions

    def DownloadActivity(self, svcRecord, activity):
        if activity.ServiceData["Manual"]:  # I should really add a param to DownloadActivity for this value as opposed to constantly doing this
            # We've got as much information as we're going to get - we need to copy it into a Lap though.
            activity.Laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]
            return activity
        activityID = activity.ServiceData["ActivityID"]

        streamdata = requests.get("https://www.strava.com/api/v3/activities/" + str(activityID) + "/streams/time,altitude,heartrate,cadence,watts,temp,moving,latlng", headers=self._apiHeaders(svcRecord))
        if streamdata.status_code == 401:
            self._logAPICall("download", (svcRecord.ExternalID, str(activity.StartTime)), "auth")
            raise APIException("No authorization to download activity", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        try:
            streamdata = streamdata.json()
        except:
            raise APIException("Stream data returned is not JSON")

        if "message" in streamdata and streamdata["message"] == "Record Not Found":
            self._logAPICall("download", (svcRecord.ExternalID, str(activity.StartTime)), "missing")
            raise APIException("Could not find activity")

        ridedata = {}
        for stream in streamdata:
            ridedata[stream["type"]] = stream["data"]

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime) # Strava doesn't support laps, but we need somewhere to put the waypoints.
        activity.Laps = [lap]
        lap.Waypoints = []

        hasHR = "heartrate" in ridedata and len(ridedata["heartrate"]) > 0
        hasCadence = "cadence" in ridedata and len(ridedata["cadence"]) > 0
        hasTemp = "temp" in ridedata and len(ridedata["temp"]) > 0
        hasPower = ("watts" in ridedata and len(ridedata["watts"]) > 0)
        hasAltitude = "altitude" in ridedata and len(ridedata["altitude"]) > 0
        hasMovingData = "moving" in ridedata and len(ridedata["moving"]) > 0
        moving = True

        if "error" in ridedata:
            self._logAPICall("download", (svcRecord.ExternalID, str(activity.StartTime)), "data")
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
            elif hasMovingData and not moving and ridedata["moving"][idx] is True:
                waypoint.Type = WaypointType.Resume
                moving = True
            elif hasMovingData and ridedata["moving"][idx] is False:
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
            lap.Waypoints.append(waypoint)
        if not hasLocation:
            self._logAPICall("download", (svcRecord.ExternalID, str(activity.StartTime)), "faulty")
            raise APIExcludeActivity("No waypoints with location", activityId=activityID)
        self._logAPICall("download", (svcRecord.ExternalID, str(activity.StartTime)), None)
        return activity

    def UploadActivity(self, serviceRecord, activity):
        logger.info("Activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))

        if self.LastUpload is not None:
            while (datetime.now() - self.LastUpload).total_seconds() < 5:
                time.sleep(1)
                logger.debug("Inter-upload cooldown")
        source_svc = None
        if hasattr(activity, "ServiceDataCollection"):
            source_svc = str(list(activity.ServiceDataCollection.keys())[0])

        if activity.CountTotalWaypoints():
            req = {
                    "data_type": "fit",
                    "activity_name": activity.Name,
                    "description": activity.Notes, # Paul Mach said so.
                    "activity_type": self._activityTypeMappings[activity.Type],
                    "private": 1 if activity.Private else 0}

            if "fit" in activity.PrerenderedFormats:
                logger.debug("Using prerendered FIT")
                fitData = activity.PrerenderedFormats["fit"]
            else:
                activity.EnsureTZ()
                # TODO: put the fit back into PrerenderedFormats once there's more RAM to go around and there's a possibility of it actually being used.
                fitData = FITIO.Dump(activity)
            files = {"file":("tap-sync-" + activity.UID + "-" + str(os.getpid()) + ("-" + source_svc if source_svc else "") + ".fit", fitData)}

            response = requests.post("http://www.strava.com/api/v3/uploads", data=req, files=files, headers=self._apiHeaders(serviceRecord))
            if response.status_code != 201:
                if response.status_code == 401:
                    raise APIException("No authorization to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                if "duplicate of activity" in response.text:
                    logger.debug("Duplicate")
                    self.LastUpload = datetime.now()
                    return # Fine by me. The majority of these cases were caused by a dumb optimization that meant existing activities on services were never flagged as such if tapiriik didn't have to synchronize them elsewhere.
                raise APIException("Unable to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))

            upload_id = response.json()["id"]
            while not response.json()["activity_id"]:
                time.sleep(5)
                response = requests.get("http://www.strava.com/api/v3/uploads/%s" % upload_id, headers=self._apiHeaders(serviceRecord))
                logger.debug("Waiting for upload - status %s id %s" % (response.json()["status"], response.json()["activity_id"]))
                if response.json()["error"]:
                    error = response.json()["error"]
                    if "duplicate of activity" in error:
                        self.LastUpload = datetime.now()
                        logger.debug("Duplicate")
                        return # I guess we're done here?
                    raise APIException("Strava failed while processing activity - last status %s" % response.text)
        else:
            localUploadTS = activity.StartTime.strftime("%Y-%m-%d %H:%M:%S")
            req = {
                    "name": activity.Name if activity.Name else activity.StartTime.strftime("%d/%m/%Y"), # This is required
                    "description": activity.Notes,
                    "type": self._activityTypeMappings[activity.Type],
                    "private": 1 if activity.Private else 0,
                    "start_date_local": localUploadTS,
                    "distance": activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value,
                    "elapsed_time": round((activity.EndTime - activity.StartTime).total_seconds())
                }
            headers = self._apiHeaders(serviceRecord)
            response = requests.post("https://www.strava.com/api/v3/activities", data=req, headers=headers)
            # FFR this method returns the same dict as the activity listing, as REST services are wont to do.
            if response.status_code != 201:
                if response.status_code == 401:
                    raise APIException("No authorization to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to upload stationary activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))
        self.LastUpload = datetime.now()

    def DeleteCachedData(self, serviceRecord):
        cachedb.strava_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.strava_activity_cache.remove({"Owner": serviceRecord.ExternalID})
