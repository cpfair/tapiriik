from tapiriik.settings import WEB_ROOT, STRAVA_CLIENT_SECRET, STRAVA_CLIENT_ID, STRAVA_RATE_LIMITS
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatistics, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.fit import FITIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import calendar
import requests
import os
import logging
import pytz
import re
import time
import json

logger = logging.getLogger(__name__)

class StravaService(ServiceBase):
    ID = "strava"
    DisplayName = "Strava"
    DisplayAbbreviation = "STV"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://www.strava.com/athletes/{0}"
    UserActivityURL = "http://app.strava.com/activities/{1}"
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.
    PartialSyncRequiresTrigger = True
    LastUpload = None

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    SupportsActivityDeletion = True

    # For mapping common->Strava; no ambiguity in Strava activity type
    _activityTypeMappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.MountainBiking: "Ride",
        ActivityType.Hiking: "Hike",
        ActivityType.Running: "Run",
        ActivityType.Walking: "Walk",
        ActivityType.Snowboarding: "Snowboard",
        ActivityType.Skating: "IceSkate",
        ActivityType.CrossCountrySkiing: "NordicSki",
        ActivityType.DownhillSkiing: "AlpineSki",
        ActivityType.Swimming: "Swim",
        ActivityType.Gym: "Workout",
        ActivityType.Rowing: "Rowing",
        ActivityType.Elliptical: "Elliptical",
        ActivityType.RollerSkiing: "RollerSki",
        ActivityType.StrengthTraining: "WeightTraining",
        ActivityType.Climbing: "RockClimbing",
    }

    # For mapping Strava->common
    _reverseActivityTypeMappings = {
        "Ride": ActivityType.Cycling,
        "VirtualRide": ActivityType.Cycling,
        "EBikeRide": ActivityType.Cycling,
        "MountainBiking": ActivityType.MountainBiking,
        "Run": ActivityType.Running,
        "Hike": ActivityType.Hiking,
        "Walk": ActivityType.Walking,
        "AlpineSki": ActivityType.DownhillSkiing,
        "CrossCountrySkiing": ActivityType.CrossCountrySkiing,
        "NordicSki": ActivityType.CrossCountrySkiing,
        "BackcountrySki": ActivityType.DownhillSkiing,
        "Snowboard": ActivityType.Snowboarding,
        "Swim": ActivityType.Swimming,
        "IceSkate": ActivityType.Skating,
        "Workout": ActivityType.Gym,
        "Rowing": ActivityType.Rowing,
        "Kayaking": ActivityType.Rowing,
        "Canoeing": ActivityType.Rowing,
        "StandUpPaddling": ActivityType.Rowing,
        "Elliptical": ActivityType.Elliptical,
        "RollerSki": ActivityType.RollerSkiing,
        "WeightTraining": ActivityType.StrengthTraining,
        "RockClimbing" : ActivityType.Climbing,
    }

    SupportedActivities = list(_activityTypeMappings.keys())

    GlobalRateLimits = STRAVA_RATE_LIMITS

    def UserUploadedActivityURL(self, uploadId):
        return "https://www.strava.com/activities/%d" % uploadId

    def WebInit(self):
        params = {'scope':'write,view_private',
                  'client_id':STRAVA_CLIENT_ID,
                  'response_type':'code',
                  'redirect_uri':WEB_ROOT + reverse("oauth_return", kwargs={"service": "strava"})}
        self.UserAuthorizationURL = \
           "https://www.strava.com/oauth/authorize?" + urlencode(params)

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "strava"})}

        response = requests.post("https://www.strava.com/oauth/token", data=params)
        if response.status_code != 200:
            raise APIException("Invalid code")
        data = response.json()

        authorizationData = {"OAuthToken": data["access_token"]}
        # Retrieve the user ID, meh.
        id_resp = requests.get("https://www.strava.com/api/v3/athlete", headers=self._apiHeaders(ServiceRecord({"Authorization": authorizationData})))
        return (id_resp.json()["id"], authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        resp = requests.post("https://www.strava.com/oauth/deauthorize", headers=self._apiHeaders(serviceRecord))
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize Strava auth token, status " + str(resp.status_code) + " resp " + resp.text)
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
            if resp.status_code == 401:
                raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            earliestDate = None

            try:
                reqdata = resp.json()
            except ValueError:
                raise APIException("Failed parsing strava list response %s - %s" % (resp.status_code, resp.text))

            if not len(reqdata):
                break  # No more activities to see

            for ride in reqdata:
                activity = UploadedActivity()
                activity.TZ = pytz.timezone(re.sub("^\([^\)]+\)\s*", "", ride["timezone"]))  # Comes back as "(GMT -13:37) The Stuff/We Want""
                activity.StartTime = pytz.utc.localize(datetime.strptime(ride["start_date"], "%Y-%m-%dT%H:%M:%SZ"))
                logger.debug("\tActivity s/t %s: %s" % (activity.StartTime, ride["name"]))
                if not earliestDate or activity.StartTime < earliestDate:
                    earliestDate = activity.StartTime
                    before = calendar.timegm(activity.StartTime.astimezone(pytz.utc).timetuple())

                activity.EndTime = activity.StartTime + timedelta(0, ride["elapsed_time"])
                activity.ServiceData = {"ActivityID": ride["id"], "Manual": ride["manual"]}

                if ride["type"] not in self._reverseActivityTypeMappings:
                    exclusions.append(APIExcludeActivity("Unsupported activity type %s" % ride["type"], activity_id=ride["id"], user_exception=UserException(UserExceptionType.Other)))
                    logger.debug("\t\tUnknown activity")
                    continue

                activity.Type = self._reverseActivityTypeMappings[ride["type"]]
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=ride["distance"])
                if "max_speed" in ride or "average_speed" in ride:
                    activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=ride["average_speed"] if "average_speed" in ride else None, max=ride["max_speed"] if "max_speed" in ride else None)
                activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=ride["moving_time"] if "moving_time" in ride and ride["moving_time"] > 0 else None)  # They don't let you manually enter this, and I think it returns 0 for those activities.
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
                activity.Stationary = ride["manual"]
                activity.GPS = ("start_latlng" in ride) and (ride["start_latlng"] is not None)
                activity.AdjustTZ()
                activity.CalculateUID()
                activities.append(activity)

            if not exhaustive or not earliestDate:
                break

        return activities, exclusions

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        # There is no per-user webhook subscription with Strava.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(True)

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        # As above.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(False)

    def ExternalIDsForPartialSyncTrigger(self, req):
        data = json.loads(req.body.decode("UTF-8"))
        return [data["owner_id"]]

    def PartialSyncTriggerGET(self, req):
        # Strava requires this endpoint to echo back a challenge.
        # Only happens once during manual endpoint setup?
        from django.http import HttpResponse
        return HttpResponse(json.dumps({
            "hub.challenge": req.GET["hub.challenge"]
        }))

    def DownloadActivity(self, svcRecord, activity):
        if activity.ServiceData["Manual"]:  # I should really add a param to DownloadActivity for this value as opposed to constantly doing this
            # We've got as much information as we're going to get - we need to copy it into a Lap though.
            activity.Laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]
            return activity
        activityID = activity.ServiceData["ActivityID"]

        streamdata = requests.get("https://www.strava.com/api/v3/activities/" + str(activityID) + "/streams/time,altitude,heartrate,cadence,watts,temp,moving,latlng,distance,velocity_smooth", headers=self._apiHeaders(svcRecord))
        if streamdata.status_code == 401:
            raise APIException("No authorization to download activity", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        try:
            streamdata = streamdata.json()
        except:
            raise APIException("Stream data returned is not JSON")

        if "message" in streamdata and streamdata["message"] == "Record Not Found":
            raise APIException("Could not find activity")

        ridedata = {}
        for stream in streamdata:
            ridedata[stream["type"]] = stream["data"]

        if "error" in ridedata:
            raise APIException("Strava error " + ridedata["error"])

        activity.Laps = []

        lapsdata = requests.get("https://www.strava.com/api/v3/activities/{}/laps".format(activityID), headers=self._apiHeaders(svcRecord))

        for lapdata in lapsdata.json():

            lapWaypoints, lapStats = self._process_lap_waypoints(activity, ridedata, lapdata)

            lapStart = pytz.utc.localize(datetime.strptime(lapdata["start_date"], "%Y-%m-%dT%H:%M:%SZ"))
            lapEnd = lapStart + timedelta(0, lapdata["elapsed_time"])
            lap = Lap(startTime=lapStart, endTime=lapEnd)
            lap.Waypoints = lapWaypoints
            
            # In single-lap case lap stats needs to match global stats
            lap.Stats = activity.Stats if len(lapsdata.json()) == 1 else lapStats

            activity.Laps.append(lap)

        return activity

    def _process_lap_waypoints(self, activity, ridedata, lapdata):

        hasHR = "heartrate" in ridedata and len(ridedata["heartrate"]) > 0
        hasCadence = "cadence" in ridedata and len(ridedata["cadence"]) > 0
        hasTemp = "temp" in ridedata and len(ridedata["temp"]) > 0
        hasPower = ("watts" in ridedata and len(ridedata["watts"]) > 0)
        hasAltitude = "altitude" in ridedata and len(ridedata["altitude"]) > 0
        hasDistance = "distance" in ridedata and len(ridedata["distance"]) > 0
        hasVelocity = "velocity_smooth" in ridedata and len(ridedata["velocity_smooth"]) > 0

        inPause = False
        waypointCt = len(ridedata["time"])

        lapWaypoints = []
        waypoinStartIndex = lapdata["start_index"]
        waypoinEndIndex = lapdata["end_index"]

        powerSum = 0
        hrSum = 0
        hrMax = 0

        for idx in range(waypoinStartIndex, waypoinEndIndex):

            waypoint = Waypoint(activity.StartTime + timedelta(0, ridedata["time"][idx]))
            if "latlng" in ridedata:
                latlng = ridedata["latlng"][idx]
                waypoint.Location = Location(latlng[0], latlng[1], None)
                if waypoint.Location.Longitude == 0 and waypoint.Location.Latitude == 0:
                    waypoint.Location.Longitude = None
                    waypoint.Location.Latitude = None

            if hasAltitude:
                if not waypoint.Location:
                    waypoint.Location = Location(None, None, None)
                waypoint.Location.Altitude = float(ridedata["altitude"][idx])

            # When pausing, Strava sends this format:
            # idx = 100 ; time = 1000; moving = true
            # idx = 101 ; time = 1001; moving = true  => convert to Pause
            # idx = 102 ; time = 2001; moving = false => convert to Resume: (2001-1001) seconds pause
            # idx = 103 ; time = 2002; moving = true

            if idx == 0:
                waypoint.Type = WaypointType.Start
            elif idx == waypointCt - 2:
                waypoint.Type = WaypointType.End
            elif idx < waypointCt - 2 and ridedata["moving"][idx+1] and inPause:
                waypoint.Type = WaypointType.Resume
                inPause = False
            elif idx < waypointCt - 2 and not ridedata["moving"][idx+1] and not inPause:
                waypoint.Type = WaypointType.Pause
                inPause = True

            if hasHR:
                waypoint.HR = ridedata["heartrate"][idx]
                hrSum += waypoint.HR
                hrMax = waypoint.HR if waypoint.HR > hrMax else hrMax
            if hasCadence:
                waypoint.Cadence = ridedata["cadence"][idx]
            if hasTemp:
                waypoint.Temp = ridedata["temp"][idx]
            if hasPower:
                waypoint.Power = ridedata["watts"][idx]
                powerSum += waypoint.Power
            if hasVelocity:
                waypoint.Speed = ridedata["velocity_smooth"][idx]
            if hasDistance:
                waypoint.Distance = ridedata["distance"][idx]
            lapWaypoints.append(waypoint)

        pointsCount = len(lapWaypoints)
        stats = ActivityStatistics()

        stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=lapdata["distance"])
        if "max_speed" in lapdata or "average_speed" in lapdata:
            stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=lapdata["average_speed"] if "average_speed" in lapdata else None, max=lapdata["max_speed"] if "max_speed" in lapdata else None)
        stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=lapdata["moving_time"] if "moving_time" in lapdata and lapdata["moving_time"] > 0 else None)
        if "average_cadence" in lapdata:
            stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=lapdata["average_cadence"]))
        if hasHR:
            stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=hrSum / pointsCount, max=hrMax))
        if hasPower:
            stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=powerSum / pointsCount)
        
        return lapWaypoints, stats

    def UploadActivity(self, serviceRecord, activity):
        logger.info("Activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))

        if self.LastUpload is not None:
            while (datetime.now() - self.LastUpload).total_seconds() < 5:
                time.sleep(1)
                logger.debug("Inter-upload cooldown")
        source_svc = None
        if hasattr(activity, "ServiceDataCollection"):
            source_svc = str(list(activity.ServiceDataCollection.keys())[0])

        upload_id = None
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
                # TODO: put the fit back into PrerenderedFormats once there's more RAM to go around and there's a possibility of it actually being used.
                fitData = FITIO.Dump(activity, drop_pauses=True)
            files = {"file":("tap-sync-" + activity.UID + "-" + str(os.getpid()) + ("-" + source_svc if source_svc else "") + ".fit", fitData)}

            response = requests.post("https://www.strava.com/api/v3/uploads", data=req, files=files, headers=self._apiHeaders(serviceRecord))
            if response.status_code != 201:
                if response.status_code == 401:
                    raise APIException("No authorization to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                if "duplicate of activity" in response.text:
                    logger.debug("Duplicate")
                    self.LastUpload = datetime.now()
                    return # Fine by me. The majority of these cases were caused by a dumb optimization that meant existing activities on services were never flagged as such if tapiriik didn't have to synchronize them elsewhere.
                raise APIException("Unable to upload activity " + activity.UID + " response " + response.text + " status " + str(response.status_code))

            upload_id = response.json()["id"]
            upload_poll_wait = 8 # The mode of processing times
            while not response.json()["activity_id"]:
                time.sleep(upload_poll_wait)
                response = requests.get("https://www.strava.com/api/v3/uploads/%s" % upload_id, headers=self._apiHeaders(serviceRecord))
                logger.debug("Waiting for upload - status %s id %s" % (response.json()["status"], response.json()["activity_id"]))
                if response.json()["error"]:
                    error = response.json()["error"]
                    if "duplicate of activity" in error:
                        self.LastUpload = datetime.now()
                        logger.debug("Duplicate")
                        return # I guess we're done here?
                    raise APIException("Strava failed while processing activity - last status %s" % response.text)
            upload_id = response.json()["activity_id"]
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
            upload_id = response.json()["id"]

        self.LastUpload = datetime.now()
        return upload_id

    def DeleteCachedData(self, serviceRecord):
        cachedb.strava_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.strava_activity_cache.remove({"Owner": serviceRecord.ExternalID})

    def DeleteActivity(self, serviceRecord, uploadId):
        headers = self._apiHeaders(serviceRecord)
        del_res = requests.delete("https://www.strava.com/api/v3/activities/%d" % uploadId, headers=headers)
        del_res.raise_for_status()
