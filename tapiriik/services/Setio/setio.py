#
#   Created by Christian Toft Andersen 2017 for SETIO @
#
from tapiriik.settings import WEB_ROOT, SETIO_CLIENT_SECRET, SETIO_CLIENT_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.fit import FITIO
from datetime import datetime, timedelta

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
import dateutil.parser

logger = logging.getLogger(__name__)

class SetioService(ServiceBase):
    ID = "setio"
    DisplayName = "Setio"
    DisplayAbbreviation = "SET"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.
    PartialSyncRequiresTrigger = True
    LastUpload = None

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    SupportsActivityDeletion = True

    # For mapping common->Setio; no ambiguity in Setio activity type
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
    }

    # For mapping Setio->common
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
    }

    SupportedActivities = list(_activityTypeMappings.keys())

    def WebInit(self):
        params = {'scope':'write,view_private',
                  'client_id':SETIO_CLIENT_ID,
                  'response_type':'code',
                  'redirect_uri':WEB_ROOT + reverse("oauth_return", kwargs={"service": "setio"})}
        self.UserAuthorizationURL = \
            "https://setio.run/oauth/authorize?" + urlencode(params)

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        authorizationData = {"OAuthToken": code}
        return (code, authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens setio distributes :\
        pass


    def DownloadActivityList(self, svcRecord, exhaustive=False):
        activities = []
        exclusions = []

        url = "https://us-central1-project-2489250248063150762.cloudfunctions.net/getRunsByUserId"

        extID = svcRecord.ExternalID

        payload = "{\"userId\": \"" + extID + "\"}"
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        try:
            reqdata = response.json()
        except ValueError:
            raise APIException("Failed parsing Setio list response %s - %s" % (resp.status_code, resp.text))


        for ride in reqdata:
            activity = UploadedActivity()

            activity.StartTime = datetime.strptime(datetime.utcfromtimestamp(ride["startTimeStamp"]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
            if "stopTimeStamp" in ride:
                activity.EndTime = datetime.strptime(datetime.utcfromtimestamp(ride["stopTimeStamp"]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
            activity.ServiceData = {"ActivityID": ride["runId"], "Manual": "False"}

           # if ride["type"] not in self._reverseActivityTypeMappings:
            if "Run" not in self._reverseActivityTypeMappings:
                exclusions.append(
                    APIExcludeActivity("Unsupported activity type %s" % "Run", activity_id=ride["runId"],
                                       user_exception=UserException(UserExceptionType.Other)))
                logger.debug("\t\tUnknown activity")
                continue

            activity.Name = ride["programName"]

            activity.Type = ActivityType.Running
            if "totalDistance" in ride:
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=ride["totalDistance"])

            if "averageCadence" in ride:
                activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=ride["averageCadence"]))

            if "averageSpeed" in ride:
                activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=ride["averageSpeed"])

            #get comment
            url = "https://us-central1-project-2489250248063150762.cloudfunctions.net/getRunComment"
            payload = "{\"userId\": \"" + extID + "\",\"runId\":\"" + activity.ServiceData["ActivityID"] + "\"}"
            headers = {
                'content-type': "application/json",
                'cache-control': "no-cache",
            }
            streamdata = requests.request("POST", url, data=payload, headers=headers)
            if streamdata.status_code == 500:
                raise APIException("Internal server error")

            if streamdata.status_code == 403:
                raise APIException("No authorization to download activity", block=True,
                                   user_exception=UserException(UserExceptionType.Authorization,
                                                                intervention_required=True))

            if streamdata.status_code != 204: # "Record Not Found":
                try:
                    commentdata = streamdata.json()
                except:
                    raise APIException("Stream data returned is not JSON")

                if "comment" in commentdata:
                    activity.Notes = commentdata["comment"]
                else:
                    activity.Notes = ""
            else:
                activity.Notes = ""

            activity.GPS = True

            activity.Private = False
            activity.Stationary = False #True = no sensor data

            activity.CalculateUID()
            activities.append(activity)

        return activities, exclusions


    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        # There is no per-user webhook subscription with Setio.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(True)

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
    #     # As above.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(False)

    def DownloadActivity(self, svcRecord, activity):
        # if activity.ServiceData["Manual"]:
        #    # Maybe implement later
        #    activity.Laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]
        #    return activity
        activityID = activity.ServiceData["ActivityID"]

        extID = svcRecord.ExternalID

        url = "https://us-central1-project-2489250248063150762.cloudfunctions.net/getRunData"
        payload = "{\"userId\": \"" + extID + "\",\"runId\":\"" + str(activityID) + "\"}"
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }
        streamdata = requests.request("POST", url, data=payload, headers=headers)
        if streamdata.status_code == 500:
            raise APIException("Internal server error")

        if streamdata.status_code == 403:
            raise APIException("No authorization to download activity", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        if streamdata.status_code == 204: #"Record Not Found":
            raise APIException("Could not find rundata")

        try:
            streamdata = streamdata.json()
        except:
            raise APIException("Stream data returned is not JSON")

        ridedata = {}

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime) # Setio doesn't support laps, but we need somewhere to put the waypoints.
        activity.Laps = [lap]
        lap.Waypoints = []

        wayPointExist = False

        countWayPoints = 0
        for stream in streamdata:
            waypoint = Waypoint(dateutil.parser.parse(stream["time"], ignoretz=True))

            if "latitude" in stream:
                if "longitude" in stream:
                    latitude = stream["latitude"]
                    longitude = stream["longitude"]
                    waypoint.Location = Location(latitude, longitude, None)
                    if waypoint.Location.Longitude == 0 and waypoint.Location.Latitude == 0:
                        waypoint.Location.Longitude = None
                        waypoint.Location.Latitude = None

            if "cadence" in stream:
                waypoint.Cadence = stream["cadence"]
                if waypoint.Cadence > 0:
                    waypoint.Cadence = waypoint.Cadence / 2

            if "elevation" in stream:
                if not waypoint.Location:
                    waypoint.Location = Location(None, None, None)
                waypoint.Location.Altitude = stream["elevation"]

            if "distance" in stream:
                waypoint.Distance = stream["distance"]
            if "speed" in stream:
                waypoint.Speed = stream["speed"]
            waypoint.Type = WaypointType.Regular
            lap.Waypoints.append(waypoint)
            countWayPoints = countWayPoints + 1

        if countWayPoints < 60:
            lap.Waypoints = []
        return activity


    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        pass
