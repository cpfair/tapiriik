#
#   Created by Christian Toft Andersen 2017 for SINGLETRACKER / MOTINNO @
#
from tapiriik.settings import WEB_ROOT, SINGLETRACKER_CLIENT_SECRET, SINGLETRACKER_CLIENT_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, \
    Waypoint, WaypointType, Location, Lap

from django.core.urlresolvers import reverse
from datetime import datetime
from urllib.parse import urlencode
import requests
import logging
import dateutil.parser
import json

logger = logging.getLogger(__name__)


class SingletrackerService(ServiceBase):
    ID = "singletracker"
    DisplayName = "Singletracker"
    DisplayAbbreviation = "SGL"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.
    PartialSyncRequiresTrigger = False
    LastUpload = None
    SingletrackerDomain = "https://us-central1-sweltering-inferno-5571.cloudfunctions.net/"

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    SupportsActivityDeletion = True

    # For mapping common->SINGLETRACKER; no ambiguity in Singletracker activity type
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

    # For mapping SINGLETRACKER->common
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
        params = {'scope': 'write,view_private',
                  'client_id': SINGLETRACKER_CLIENT_ID,
                  'response_type': 'code',
                  'redirect_uri': WEB_ROOT + reverse("oauth_return", kwargs={"service": "singletracker"})}
        self.UserAuthorizationURL = \
            "https://singletracker.dk/oauth/authorize/?" + urlencode(params)

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        authorizationData = {"OAuthToken": code}
        return (code, authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens singletracker distributes :\
        pass

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        activities = []
        exclusions = []

        url = self.SingletrackerDomain + "getRidesByUserId"
        extID = svcRecord.ExternalID

        payload = {"userId": extID}
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        try:
            reqdata = response.json()
        except ValueError:
            raise APIException("Failed parsing Singletracker list response %s - %s" % (resp.status_code, resp.text))

        for ride in reqdata:
            activity = UploadedActivity()
            activity.StartTime = datetime.strptime(
                datetime.utcfromtimestamp(ride["startTime"]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
            if "stopTime" in ride:
                activity.EndTime = datetime.strptime(
                    datetime.utcfromtimestamp(ride["stopTime"]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
            activity.ServiceData = {"ActivityID": ride["rideId"], "Manual": "False"}

            activity.Name = ride["trackName"]

            logger.debug("\tActivity s/t %s: %s" % (activity.StartTime, activity.Name))
            activity.Type = ActivityType.MountainBiking
            if "totalDistance" in ride:
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=ride["totalDistance"])

            if "avgSpeed" in ride:
                activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond,
                                                         avg=ride["avgSpeed"])
            activity.Notes = None

            activity.GPS = True

            activity.Private = False
            activity.Stationary = False  # True = no sensor data

            activity.CalculateUID()
            activities.append(activity)

        return activities, exclusions

    def DownloadActivity(self, svcRecord, activity):

        activityID = activity.ServiceData["ActivityID"]
        extID = svcRecord.ExternalID
        url = self.SingletrackerDomain + "getRideData"

        payload = {"userId": extID, "rideId": activityID}
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }
        streamdata = requests.post(url, data=json.dumps(payload), headers=headers)
        if streamdata.status_code == 500:
            raise APIException("Internal server error")

        if streamdata.status_code == 403:
            raise APIException("No authorization to download activity", block=True,
                               user_exception=UserException(UserExceptionType.Authorization,
                                                            intervention_required=True))
        if streamdata.status_code == 200:  # Ok
            try:
                streamdata = streamdata.json()
            except:
                raise APIException("Stream data returned is not JSON")

        ridedata = {}

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime,
                  endTime=activity.EndTime)  # Singletracker doesn't support laps, but we need somewhere to put the waypoints.
        activity.Laps = [lap]
        lap.Waypoints = []

        wayPointExist = False

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

        return activity

    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        pass
