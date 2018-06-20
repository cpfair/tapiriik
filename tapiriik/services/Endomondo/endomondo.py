from tapiriik.settings import WEB_ROOT, ENDOMONDO_CLIENT_KEY, ENDOMONDO_CLIENT_SECRET, SECRET_KEY
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.database import redis

from django.core.urlresolvers import reverse
from datetime import timedelta, datetime
import dateutil.parser
from requests_oauthlib import OAuth1Session
import logging
import pytz
import json
import os
import hashlib

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    DisplayAbbreviation = "EN"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "https://www.endomondo.com/profile/{0}"
    UserActivityURL = "https://www.endomondo.com/users/{0}/workouts/{1}"

    PartialSyncRequiresTrigger = True
    AuthenticationNoFrame = True

    ConfigurationDefaults = {
        "DeviceRegistered": False,
    }

    # The complete list:
    # running,cycling transportation,cycling sport,mountain biking,skating,roller skiing,skiing cross country,skiing downhill,snowboarding,kayaking,kite surfing,rowing,sailing,windsurfing,fitness walking,golfing,hiking,orienteering,walking,riding,swimming,spinning,other,aerobics,badminton,baseball,basketball,boxing,stair climbing,cricket,cross training,dancing,fencing,american football,rugby,soccer,handball,hockey,pilates,polo,scuba diving,squash,table tennis,tennis,beach volley,volleyball,weight training,yoga,martial arts,gymnastics,step counter,crossfit,treadmill running,skateboarding,surfing,snowshoeing,wheelchair,climbing,treadmill walking,kick scooter,standup paddling,running trail,rowing indoor,floorball,ice skating,skiing touring,rope jumping,stretching,running canicross,paddle tennis,paragliding
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
        "hiking": ActivityType.Hiking,
        "orienteering": ActivityType.Running,
        "walking": ActivityType.Walking,
        "swimming": ActivityType.Swimming,
        "spinning": ActivityType.Cycling, # indoor cycling
        "other": ActivityType.Other,
        "cross training": ActivityType.Elliptical, # elliptical training
        "weight training": ActivityType.StrengthTraining,
        "treadmill running": ActivityType.Running,
        "snowshoeing": ActivityType.Walking,
        "wheelchair": ActivityType.Wheelchair,
        "climbing": ActivityType.Climbing,
        "roller skiing": ActivityType.RollerSkiing,
        "treadmill walking": ActivityType.Walking,
        "running trail": ActivityType.Running,
        "rowing indoor": ActivityType.Rowing,
        "running canicross": ActivityType.Running,
        "stand up paddling": ActivityType.StandUpPaddling,
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
        "hiking": ActivityType.Hiking,
        "swimming": ActivityType.Swimming,
        "other": ActivityType.Other,
        "wheelchair": ActivityType.Wheelchair,
        "climbing" : ActivityType.Climbing,
        "roller skiing": ActivityType.RollerSkiing,
        "stand up paddling": ActivityType.StandUpPaddling,
    }
    
    _activitiesThatDontRoundTrip = {
        ActivityType.Cycling,
        ActivityType.Running,
        ActivityType.Walking
    }

    SupportedActivities = list(_activityMappings.values())

    ReceivesNonGPSActivitiesWithOtherSensorData = False

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "endomondo"})

    def _rateLimitBailout(self, response):
        if response.status_code == 503 and "user_refused" in response.text:
            raise APIException("Endomondo user token rate limit reached", user_exception=UserException(UserExceptionType.RateLimited))

    def _oauthSession(self, connection=None, **params):
        if connection:
            params["resource_owner_key"] = connection.Authorization["Token"]
            params["resource_owner_secret"] = connection.Authorization["Secret"]
        return OAuth1Session(ENDOMONDO_CLIENT_KEY, client_secret=ENDOMONDO_CLIENT_SECRET, **params)

    def GenerateUserAuthorizationURL(self, session, level=None):
        oauthSession = self._oauthSession(callback_uri=WEB_ROOT + reverse("oauth_return", kwargs={"service": "endomondo"}))
        tokens = oauthSession.fetch_request_token("https://api.endomondo.com/oauth/request_token")
        redis_token_key = 'endomondo:oauth:%s' % tokens["oauth_token"]
        redis.setex(redis_token_key, tokens["oauth_token_secret"], timedelta(hours=24))
        return oauthSession.authorization_url("https://www.endomondo.com/oauth/authorize")

    def RetrieveAuthorizationToken(self, req, level):
        redis_token_key = "endomondo:oauth:%s" % req.GET["oauth_token"]
        secret = redis.get(redis_token_key)
        assert secret
        redis.delete(redis_token_key)
        oauthSession = self._oauthSession(resource_owner_secret=secret)
        oauthSession.parse_authorization_response(req.get_full_path())
        tokens = oauthSession.fetch_access_token("https://api.endomondo.com/oauth/access_token")
        userInfo = oauthSession.get("https://api.endomondo.com/api/1/user")
        userInfo = userInfo.json()
        return (userInfo["id"], {"Token": tokens["oauth_token"], "Secret": tokens["oauth_token_secret"]})

    def RevokeAuthorization(self, serviceRecord):
        pass

    def _parseDate(self, date):
        return datetime.strptime(date, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=pytz.utc)

    def _formatDate(self, date):
        return datetime.strftime(date.astimezone(pytz.utc), "%Y-%m-%d %H:%M:%S UTC")

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        oauthSession = self._oauthSession(serviceRecord)

        activities = []
        exclusions = []

        page_url = "https://api.endomondo.com/api/1/workouts"

        while True:
            resp = oauthSession.get(page_url)
            try:
                respList = resp.json()["data"]
            except ValueError:
                self._rateLimitBailout(resp)
                raise APIException("Error decoding activity list resp %s %s" % (resp.status_code, resp.text))
            for actInfo in respList:
                activity = UploadedActivity()
                activity.StartTime = self._parseDate(actInfo["start_time"])
                logger.debug("Activity s/t %s" % activity.StartTime)
                if "is_tracking" in actInfo and actInfo["is_tracking"]:
                    exclusions.append(APIExcludeActivity("Not complete", activity_id=actInfo["id"], permanent=False, user_exception=UserException(UserExceptionType.LiveTracking)))
                    continue

                if "end_time" in actInfo:
                    activity.EndTime = self._parseDate(actInfo["end_time"])

                if actInfo["sport"] in self._activityMappings:
                    activity.Type = self._activityMappings[actInfo["sport"]]

                # "duration" is timer time
                if "duration_total" in actInfo:
                    activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(actInfo["duration_total"]))

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

                activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour)
                if "speed_max" in actInfo:
                    activity.Stats.Speed.Max = float(actInfo["speed_max"])

                if "heart_rate_avg" in actInfo:
                    activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(actInfo["heart_rate_avg"]))

                if "heart_rate_max" in actInfo:
                    activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=float(actInfo["heart_rate_max"])))

                if "cadence_avg" in actInfo:
                    activity.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=int(actInfo["cadence_avg"]))

                if "cadence_max" in actInfo:
                    activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, max=int(actInfo["cadence_max"])))

                if "power_avg" in actInfo:
                    activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=int(actInfo["power_avg"]))

                if "power_max" in actInfo:
                    activity.Stats.Power.update(ActivityStatistic(ActivityStatisticUnit.Watts, max=int(actInfo["power_max"])))

                if "title" in actInfo:
                    activity.Name = actInfo["title"]

                activity.ServiceData = {"WorkoutID": int(actInfo["id"]), "Sport": actInfo["sport"]}

                activity.CalculateUID()
                activities.append(activity)

            paging = resp.json()["paging"]
            if "next" not in paging or not paging["next"] or not exhaustive:
                break
            else:
                page_url = paging["next"]

        return activities, exclusions

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).put("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        try:
            assert resp.status_code in [200, 201] # Created, or already existed
        except:
            raise APIException("Could not unsubscribe - received unknown result %s - %s" % (resp.status_code, resp.text))
        serviceRecord.SetPartialSyncTriggerSubscriptionState(True)

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        resp = self._oauthSession(serviceRecord).delete("https://api.endomondo.com/api/1/subscriptions/workout/%s" % serviceRecord.ExternalID)
        try:
            assert resp.status_code in [204, 500] # Docs say otherwise, but no-subscription-found is 500
        except:
            raise APIException("Could not unsubscribe - received unknown result %s - %s" % (resp.status_code, resp.text))
        serviceRecord.SetPartialSyncTriggerSubscriptionState(False)

    def ExternalIDsForPartialSyncTrigger(self, req):
        data = json.loads(req.body.decode("UTF-8"))
        delta_external_ids = [int(x["id"]) for x in data["data"]]
        return delta_external_ids

    def DownloadActivity(self, serviceRecord, activity):
        resp = self._oauthSession(serviceRecord).get("https://api.endomondo.com/api/1/workouts/%d" % activity.ServiceData["WorkoutID"], params={"fields": "points"})
        try:
            resp = resp.json()
        except ValueError:
            self._rateLimitBailout(resp)
            res_txt = resp.text
            raise APIException("Parse failure in Endomondo activity download: %s" % resp.status_code)
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        activity.GPS = False

        old_location = None
        in_pause = False
        for pt in resp["points"]:
            wp = Waypoint()
            if "time" not in pt:
                # Manually-entered activities with a course attached to them have date-less waypoints
                # It'd be nice to transfer those courses, but it's a concept few other sites support AFAIK
                # So, ignore the points entirely
                continue
            wp.Timestamp = self._parseDate(pt["time"])

            if ("lat" in pt and "lng" in pt) or "alt" in pt:
                wp.Location = Location()
                if "lat" in pt and "lng" in pt:
                    wp.Location.Latitude = pt["lat"]
                    wp.Location.Longitude = pt["lng"]
                    activity.GPS = True
                if "alt" in pt:
                    wp.Location.Altitude = pt["alt"]

                if wp.Location == old_location:
                    # We have seen the point with the same coordinates
                    # before. This causes other services (e.g Strava) to
                    # interpret this as if we were standing for a while,
                    # which causes us having wrong activity time when
                    # importing. We mark the point as paused in hopes this
                    # fixes the issue.
                    in_pause = True
                    wp.Type = WaypointType.Pause
                elif in_pause:
                    in_pause = False
                    wp.Type = WaypointType.Resume

                old_location = wp.Location

            if "hr" in pt:
                wp.HR = pt["hr"]

            if "cad" in pt:
                wp.Cadence = pt["cad"]

            if "pow" in pt:
                wp.Power = pt["pow"]

            lap.Waypoints.append(wp)
        activity.Stationary = len(lap.Waypoints) == 0
        return activity

    def _deviceId(self, serviceRecord):
        csp = hashlib.new("md5")
        csp.update(str(serviceRecord.ExternalID).encode("utf-8"))
        csp.update(SECRET_KEY.encode("utf-8"))
        return "tap-" + csp.hexdigest()
    
    def _getSport(self, activity):
        # This is an activity type that doesn't round trip
        if (activity.Type in self._activitiesThatDontRoundTrip and 
        # We have the original sport
        "Sport" in activity.ServiceData and 
        # We know what this sport is
        activity.ServiceData["Sport"] in self._activityMappings and 
        # The type didn't change (if we changed from Walking to Cycling, we'd want to let the new value through)
        activity.Type == self._activityMappings[activity.ServiceData["Sport"]]):
            return activity.ServiceData["Sport"]
        else:
            return [k for k,v in self._reverseActivityMappings.items() if v == activity.Type][0]

    def UploadActivity(self, serviceRecord, activity):
        session = self._oauthSession(serviceRecord)
        device_id = self._deviceId(serviceRecord)
        if not serviceRecord.GetConfiguration()["DeviceRegistered"]:
            device_info = {
                "name": "tapiriik",
                "vendor": "tapiriik",
                "model": "tapiriik",
                "os": "tapiriik",
                "os_version": "1",
                "app_variant": "tapiriik",
                "app_version": "1"
            }
            device_add_resp = session.post("https://api.endomondo.com/api/1/device/%s" % device_id, data=json.dumps(device_info))
            if device_add_resp.status_code != 200:
                self._rateLimitBailout(device_add_resp)
                raise APIException("Could not add device %s %s" % (device_add_resp.status_code, device_add_resp.text))
            serviceRecord.SetConfiguration({"DeviceRegistered": True})

        activity_id = "tap-" + activity.UID + "-" + str(os.getpid())
        
        sport = self._getSport(activity)

        upload_data = {
            "device_id": device_id,
            "sport": sport,
            "start_time": self._formatDate(activity.StartTime),
            "end_time": self._formatDate(activity.EndTime),
            "points": []
        }

        if activity.Name:
            upload_data["title"] = activity.Name

        if activity.Notes:
            upload_data["notes"] = activity.Notes

        if activity.Stats.Distance.Value is not None:
            upload_data["distance_total"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value

        if activity.Stats.TimerTime.Value is not None:
            upload_data["duration_total"] = activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        elif activity.Stats.MovingTime.Value is not None:
            upload_data["duration_total"] = activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        else:
            upload_data["duration_total"] = (activity.EndTime - activity.StartTime).total_seconds()

        if activity.Stats.Energy.Value is not None:
            upload_data["calories_total"] = activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value

        elev_stats = activity.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters)
        if elev_stats.Max is not None:
            upload_data["altitude_max"] = elev_stats.Max
        if elev_stats.Min is not None:
            upload_data["altitude_min"] = elev_stats.Min
        if elev_stats.Gain is not None:
            upload_data["total_ascent"] = elev_stats.Gain
        if elev_stats.Loss is not None:
            upload_data["total_descent"] = elev_stats.Loss

        speed_stats = activity.Stats.Speed.asUnits(ActivityStatisticUnit.KilometersPerHour)
        if speed_stats.Max is not None:
            upload_data["speed_max"] = speed_stats.Max

        hr_stats = activity.Stats.HR.asUnits(ActivityStatisticUnit.BeatsPerMinute)
        if hr_stats.Average is not None:
            upload_data["heart_rate_avg"] = hr_stats.Average
        if hr_stats.Max is not None:
            upload_data["heart_rate_max"] = hr_stats.Max

        if activity.Stats.Cadence.Average is not None:
            upload_data["cadence_avg"] = activity.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Average
        elif activity.Stats.RunCadence.Average is not None:
            upload_data["cadence_avg"] = activity.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute).Average

        if activity.Stats.Cadence.Max is not None:
            upload_data["cadence_max"] = activity.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Max
        elif activity.Stats.RunCadence.Max is not None:
            upload_data["cadence_max"] = activity.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute).Max

        if activity.Stats.Power.Average is not None:
            upload_data["power_avg"] = activity.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Average

        if activity.Stats.Power.Max is not None:
            upload_data["power_max"] = activity.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Max

        for wp in activity.GetFlatWaypoints():
            pt = {
                "time": self._formatDate(wp.Timestamp),
            }
            if wp.Location:
                if wp.Location.Latitude is not None and wp.Location.Longitude is not None:
                    pt["lat"] = wp.Location.Latitude
                    pt["lng"] = wp.Location.Longitude
                if wp.Location.Altitude is not None:
                    pt["alt"] = wp.Location.Altitude
            if wp.HR is not None:
                pt["hr"] = round(wp.HR)
            if wp.Cadence is not None:
                pt["cad"] = round(wp.Cadence)
            elif wp.RunCadence is not None:
                pt["cad"] = round(wp.RunCadence)

            if wp.Power is not None:
                pt["pow"] = round(wp.Power)

            if wp.Type == WaypointType.Pause:
                pt["inst"] = "pause"
            elif wp.Type == WaypointType.Resume:
                pt["inst"] = "resume"
            upload_data["points"].append(pt)

        if len(upload_data["points"]):
            upload_data["points"][0]["inst"] = "start"
            upload_data["points"][-1]["inst"] = "stop"

        upload_resp = session.post("https://api.endomondo.com/api/1/workouts/%s" % activity_id, data=json.dumps(upload_data))
        if upload_resp.status_code != 200:
            self._rateLimitBailout(upload_resp)
            raise APIException("Could not upload activity %s %s" % (upload_resp.status_code, upload_resp.text))

        return upload_resp.json()["id"]

    def DeleteCachedData(self, serviceRecord):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        session = self._oauthSession(serviceRecord)
        del_res = session.delete("https://api.endomondo.com/api/1/workouts/%s" % uploadId)
        del_res.raise_for_status()
