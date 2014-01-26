from tapiriik.settings import WEB_ROOT, ENDOMONDO_CLIENT_KEY, ENDOMONDO_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType

from django.core.urlresolvers import reverse
from datetime import timedelta
import dateutil.parser
from requests_oauthlib import OAuth1Session
import logging

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://www.endomondo.com/profile/{0}"
    UserActivityURL = "http://www.endomondo.com/workouts/{1}/{0}"

    PartialSyncRequiresTrigger = True
    AuthenticationNoFrame = True

    # The complete list:
    # running,cycling transportation,cycling sport,mountain biking,skating,roller skiing,skiing cross country,skiing downhill,snowboarding,kayaking,kite surfing,rowing,sailing,windsurfing,fitness walking,golfing,hiking,orienteering,walking,riding,swimming,spinning,other,aerobics,badminton,baseball,basketball,boxing,stair climbing,cricket,cross training,dancing,fencing,american football,rugby,soccer,handball,hockey,pilates,polo,scuba diving,squash,table tennis,tennis,beach volley,volleyball,weight training,yoga,martial arts,gymnastics,step counter,crossfit,treadmill running,skateboarding,surfing,snowshoeing,wheelchair,climbing,treadmill walking
    _activityMappings = {
        "running": ActivityType.Running,
        "cycling transportation": ActivityType.Cycling,
        "cycling sport": ActivityType.Cycling,
        "mountain biking": ActivityType.MountainBiking,
        "skating": ActivityType.Skating,
        "skiing cross country": ActivityType.CrossCountrySkiing,
        "skiing downhill": ActivityType.DownhillSkiing,
        "snowboarding": ActivityType.Snowboarding,
        "rowing": ActivityType.Rowing,
        "fitness walking": ActivityType.Walking,
        "hiking": ActivityType.Walking,
        "orienteering": ActivityType.Walking,
        "walking": ActivityType.Walking,
        "swimming": ActivityType.Swimming,
        "other": ActivityType.Other,
        "treadmill running": ActivityType.Running,
        "snowshoeing": ActivityType.Walking,
        "wheelchair": ActivityType.Wheelchair,
        "treadmill walking": ActivityType.Walking
    }

    _reverseActivityMappings = {
        "running": ActivityType.Running,
        "cycling sport": ActivityType.Cycling,
        "mountain biking": ActivityType.MountainBiking,
        "skating": ActivityType.Skating,
        "skiing cross country": ActivityType.CrossCountrySkiing,
        "skiing downhill": ActivityType.DownhillSkiing,
        "snowboarding": ActivityType.Snowboarding,
        "rowing": ActivityType.Rowing,
        "walking": ActivityType.Walking,
        "swimming": ActivityType.Swimming,
        "other": ActivityType.Other,
        "snowshoeing": ActivityType.Walking,
        "wheelchair": ActivityType.Wheelchair,
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways

    _oauth_token_secrets = {}

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "endomondo"})

    def _oauthSession(self, connection=None, **params):
        if connection:
            print(connection.Authorization)
            params["resource_owner_key"] = connection.Authorization["Token"]
            params["resource_owner_secret"] = connection.Authorization["Secret"]
        return OAuth1Session(ENDOMONDO_CLIENT_KEY, client_secret=ENDOMONDO_CLIENT_SECRET, **params)

    def GenerateUserAuthorizationURL(self, level=None):
        oauthSession = self._oauthSession(callback_uri=WEB_ROOT + reverse("oauth_return", kwargs={"service": "endomondo"}))
        tokens = oauthSession.fetch_request_token("https://api.endomondo.com/oauth/request_token")
        self._oauth_token_secrets[tokens["oauth_token"]] = tokens["oauth_token_secret"]
        return oauthSession.authorization_url("https://www.endomondo.com/oauth/authorize")

    def RetrieveAuthorizationToken(self, req, level):
        oauthSession = self._oauthSession(resource_owner_secret=self._oauth_token_secrets[req.GET["oauth_token"]])
        oauthSession.parse_authorization_response(req.get_full_path())
        tokens = oauthSession.fetch_access_token("https://api.endomondo.com/oauth/access_token")
        userInfo = oauthSession.get("https://api.endomondo.com/api/1/user")
        userInfo = userInfo.json()
        return (userInfo["id"], {"Token": tokens["oauth_token"], "Secret": tokens["oauth_token_secret"]})

    def RevokeAuthorization(self, serviceRecord):
        pass

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        oauthSession = self._oauthSession(serviceRecord)

        activities = []
        exclusions = []

        while True:
            resp = oauthSession.get("https://api.endomondo.com/api/1/workouts", params={"before_id": activities[-1].ServiceData["WorkoutID"] if len(activities) else None})
            respList = resp.json()["data"]
            for actInfo in respList:
                activity = UploadedActivity()
                activity.StartTime = dateutil.parser.parse(actInfo["start_time"])

                if "end_time" in actInfo:
                    activity.EndTime = dateutil.parser.parse(actInfo["end_time"])
                else:
                    continue

                if actInfo["sport"] in self._activityMappings:
                    activity.Type = self._activityMappings[actInfo["sport"]]

                # "duration" is timer time
                if "duration_total" in actInfo:
                    activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=float(actInfo["duration_total"])))

                if "distance_total" in actInfo:
                    activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(actInfo["distance_total"]))

                if "calories_total" in actInfo:
                    activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(actInfo["calories_total"]))

                activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters)

                if "altitude_max" in actInfo:
                    activity.Stats.Elevation.Max = float(actInfo["altitude_max"])

                if "altitude_min" in actInfo:
                    activity.Stats.Elevation.Min = float(actInfo["altitude_min"])

                if "total_ascent" in actInfo:
                    activity.Stats.Elevation.Gain = float(actInfo["total_ascent"])

                if "total_descent" in actInfo:
                    activity.Stats.Elevation.Loss = float(actInfo["total_descent"])

                if "speed_max" in actInfo:
                    activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, max=float(actInfo["speed_max"]))

                if "heart_rate_avg" in actInfo:
                    activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(actInfo["heart_rate_avg"]))

                if "heart_rate_max" in actInfo:
                    activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=float(actInfo["heart_rate_max"])))

                if "cadence_avg" in actInfo:
                    activity.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=int(actInfo["cadence_avg"]))

                if "cadence_max" in actInfo:
                    activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, max=int(actInfo["cadence_max"])))

                if "title" in actInfo:
                    activity.Name = actInfo["title"]

                activity.ServiceData = {"WorkoutID": int(actInfo["id"])}

                activities.append(activity)

            if not exhaustive or not len(respList):
                break

        return activities, exclusions

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).put("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        assert resp.status_code in [200, 201] # Created, or already existed

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).delete("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        assert resp.status_code in [204, 500] # Docs say otherwise, but no-subscription-found is 500

    def DownloadActivity(self, serviceRecord, activity):
        resp = self._oauthSession(serviceRecord).get("https://api.endomondo.com/api/1/workouts/%d" % activity.ServiceData["WorkoutID"], params={"fields": "points"})
        print(resp.text)
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]
        lap.Waypoints = []

        return activity

    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass
