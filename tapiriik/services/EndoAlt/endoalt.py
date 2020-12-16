from tapiriik.settings import WEB_ROOT, ENDOALT_PROXY_URL
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.fit import FITIO
from tapiriik.services.pwx import PWXIO
from tapiriik.services.tcx import TCXIO
from tapiriik.services.gpx import GPXIO
from lxml import etree

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import dateutil.parser
import requests
import time
import json
import os
import logging
import pytz

logger = logging.getLogger(__name__)

class EndoAltService(ServiceBase):
    ID = "endoalt"
    DisplayName = "Endomondo Unofficial API"
    DisplayAbbreviation = "EA"
    _urlRoot = ENDOALT_PROXY_URL
    # "http://localhost:8001"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesStationaryActivities = False

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    # The complete list:
    # running,cycling transportation,cycling sport,mountain biking,skating,roller skiing,skiing cross country,skiing downhill,snowboarding,kayaking,kite surfing,rowing,sailing,windsurfing,fitness walking,golfing,hiking,orienteering,walking,riding,swimming,spinning,other,aerobics,badminton,baseball,basketball,boxing,stair climbing,cricket,cross training,dancing,fencing,american football,rugby,soccer,handball,hockey,pilates,polo,scuba diving,squash,table tennis,tennis,beach volley,volleyball,weight training,yoga,martial arts,gymnastics,step counter,crossfit,treadmill running,skateboarding,surfing,snowshoeing,wheelchair,climbing,treadmill walking,kick scooter,standup paddling,running trail,rowing indoor,floorball,ice skating,skiing touring,rope jumping,stretching,running canicross,paddle tennis,paragliding
    
    # For mapping EndoAlt -> common
    _activityMappings = {
        #     RUNNING = 0,
        0: ActivityType.Running,
        #     CYCLING_TRANSPORT = 1,
        1: ActivityType.Cycling,
        #     CYCLING_SPORT = 2,
        1: ActivityType.Cycling,
        #     MOUNTAIN_BIKINGS = 3,
        3: ActivityType.MountainBiking,
        #     SKATING = 4,
        4: ActivityType.Skating,
        #     ROLLER_SKIING = 5,
        5: ActivityType.RollerSkiing,
        #     SKIING_CROSS_COUNTRY = 6,
        6: ActivityType.CrossCountrySkiing,
        #     SKIING_DOWNHILL = 7,
        7: ActivityType.DownhillSkiing,
        #     SNOWBOARDING = 8,
        8: ActivityType.Snowboarding,
        #     KAYAKING = 9,
        9: ActivityType.Rowing,
        #     KITE_SURFING = 10,
        10: ActivityType.Other,
        #     ROWING = 11,
        11: ActivityType.Rowing,
        #     SAILING = 12,
        12: ActivityType.Other,
        #     WINDSURFING = 13,
        13: ActivityType.Other,
        #     FINTESS_WALKING = 14,
        14: ActivityType.Walking,
        #     GOLFING = 15,
        15: ActivityType.Walking,
        #     HIKING = 16,
        16: ActivityType.Hiking,
        #     ORIENTEERING = 17,
        17: ActivityType.Running,
        #     WALKING = 18,
        18: ActivityType.Walking,
        #     RIDING = 19,
        19: ActivityType.Other,
        #     SWIMMING = 20,
        20: ActivityType.Swimming,
        #     CYCLING_INDOOR = 21,
        21: ActivityType.Elliptical,
        #     OTHER = 22,
        22: ActivityType.Other,
        #     AEROBICS = 23,
        23: ActivityType.Gym,
        #     BADMINTON = 24,
        24: ActivityType.Running,
        #     BASEBALL = 25,
        25: ActivityType.Running,
        #     BASKETBALL = 26,
        26: ActivityType.Running,
        #     BOXING = 27,
        27: ActivityType.Other,
        #     CLIMBING_STAIRS = 28,
        28: ActivityType.Other,
        #     CRICKET = 29,
        29: ActivityType.Walking,
        #     ELLIPTICAL_TRAINING = 30,
        30: ActivityType.Elliptical,
        #     DANCING = 31,
        31: ActivityType.Other,
        #     FENCING = 32,
        32: ActivityType.Other,
        #     FOOTBALL_AMERICAN = 33,
        33: ActivityType.Running,
        #     FOOTBALL_RUGBY = 34,
        34: ActivityType.Running,
        #     FOOTBALL_SOCCER = 35,
        35: ActivityType.Running,
        #     HANDBALL = 36,
        36: ActivityType.Other,
        #     HOCKEY = 37,
        37: ActivityType.Skating,
        #     PILATES = 38,
        38: ActivityType.Gym,
        #     POLO = 39,
        39: ActivityType.Other,
        #     SCUBA_DIVING = 40,
        40: ActivityType.Swimming,
        #     SQUASH = 41,
        41: ActivityType.Running,
        #     TABLE_TENIS = 42,
        42: ActivityType.Other,
        #     TENNIS = 43,
        43: ActivityType.Running,
        #     VOLEYBALL_BEACH = 44,
        44: ActivityType.Running,
        #     VOLEYBALL_INDOOR = 45,
        45: ActivityType.Running,
        #     WEIGHT_TRAINING = 46,
        46: ActivityType.Gym,
        #     YOGA = 47,
        47: ActivityType.Other,
        #     MARTINAL_ARTS = 48,
        48: ActivityType.Other,
        #     GYMNASTICS = 49,
        49: ActivityType.Gym,
        #     STEP_COUNTER = 50,
        50: ActivityType.Other,
        #     CIRKUIT_TRAINING = 87,
        51: ActivityType.Elliptical,
        #     RUNNING_TREADMILL = 88,
        52: ActivityType.Elliptical,
        #     SKATEBOARDING = 89,
        53: ActivityType.Skating,
        #     SURFING = 90,
        54: ActivityType.Skating,
        #     SNOWSHOEING = 91,
        55: ActivityType.Walking,
        #     WHEELCHAIR = 92,
        56: ActivityType.Wheelchair,
        #     CLIMBING = 93,
        57: ActivityType.Climbing,
        #     WALKING_TREADMILL = 94,
        58: ActivityType.Elliptical,
        #     KICK_SCOOTER = 95,
        59: ActivityType.Skating,
        #     STAND_UP_PADDLING = 96,
        60: ActivityType.StandUpPaddling,
        #     TRAIL_RUNNING = 97,
        61: ActivityType.Running,
        #     ROWING_INDOORS = 98,
        62: ActivityType.Elliptical,
        #     FLOORBALL = 99,
        63: ActivityType.Skating,
        #     ICE_SKATING = 100,
        64: ActivityType.Skating,
        #     SKI_TOURING = 101,
        65: ActivityType.CrossCountrySkiing,
        #     ROPE_JUMPING = 102,
        66: ActivityType.Other,
        #     STRETCHING = 103,
        67: ActivityType.Other,
        #     CANICROSS = 104,
        68: ActivityType.Other,
        #     PADDLE_TENNIS = 105,
        69: ActivityType.Other,
        #     PARAGLIDING = 106,
        70: ActivityType.Other
    }

    # For mapping common -> EndoAlt
    _reverseActivityMappings = {
        ActivityType.Running: 0,
        ActivityType.Cycling: 1,
        ActivityType.MountainBiking: 3,
        ActivityType.Walking: 18,
        ActivityType.Hiking: 16,
        ActivityType.DownhillSkiing: 7,
        ActivityType.CrossCountrySkiing: 6,
        ActivityType.Snowboarding: 8,
        ActivityType.Skating: 4,
        ActivityType.Swimming: 20,
        ActivityType.Wheelchair: 92,
        ActivityType.Rowing: 11,
        ActivityType.Elliptical: 30,
        ActivityType.Gym: 49,
        ActivityType.Climbing: 93,
        ActivityType.RollerSkiing: 5,
        ActivityType.StrengthTraining: 46,
        ActivityType.StandUpPaddling: 96,
        ActivityType.Other: 22
    }

    SupportedActivities = list(_reverseActivityMappings.keys())

    ReceivesNonGPSActivitiesWithOtherSensorData = True

    def _add_auth_params(self, params=None, record=None):
        """
        Adds username and password to the passed-in params,
        returns modified params dict.
        """

        from tapiriik.auth.credential_storage import CredentialStore

        if params is None:
            params = {}
        if record:
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            params['user'] = email
            params['pass'] = password
        return params

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})


    def Authorize(self, email, password):
        """
        POST Username and Password

        URL: http://endoalt_proxy/auth
        Parameters:
        user = username
        pass = password

        The login was successful if you get HTTP status code 200.
        For other HTTP status codes, the login was not successful.
        """

        from tapiriik.auth.credential_storage import CredentialStore

        res = requests.post(self._urlRoot + "/auth",
                           params={'user': email, 'pass': password, 'view': 'json'})

        if res.status_code != 200:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        res.raise_for_status()
        res = res.json()
        if res["token"] is None:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        member_id = res["user-id"]
        if not member_id:
            raise APIException("Unable to retrieve user id", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (member_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def _parseDate(self, date):
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)

    def _formatDate(self, date):
        return datetime.strftime(date.astimezone(pytz.utc), "%Y-%m-%d %H:%M:%S UTC")

    def _durationToSeconds(self, dur):
        parts = dur.split(":")
        return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        """
        #     GET List of Activities as JSON File

        #     URL: http://endoalt_proxy/workouts
        #     Parameters:
        #     user      = username
        #     pass      = password
        #     date_from = YYYY-MM-DD
        #     date_to   = YYYY-MM-DD
        #     """
        
        activities = []
        exclusions = []
        discoveredWorkoutIds = []

        params = self._add_auth_params({}, record=serviceRecord)

        limitDateFormat = "%Y-%m-%d"

        if exhaustive:
            listEnd   = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = datetime(day=1, month=1, year=1980) # The beginning of time
        else:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = listEnd - timedelta(days=20) # Doesn't really matter

        params.update({"date_from": listStart.strftime(limitDateFormat), "date_to": listEnd.strftime(limitDateFormat), "offset": 0, "limit": 50})

        while True:
            
            logger.debug("Requesting %s to %s" % (listStart, listEnd))
            resp = requests.get(self._urlRoot + "/workouts", params=params, stream=True )

            try:
                respList = resp.json()["workouts"]
            except ValueError:
                self._rateLimitBailout(resp)
                raise APIException("Error decoding activity list resp %s %s" % (resp.status_code, resp.text))
            
            # Empty list
            if not respList:
                break
            
            for actInfo in respList:
                actInfo = actInfo["source"]
                activity = UploadedActivity()
                activity.StartTime = self._parseDate(actInfo["start_time"])
                logger.debug("Activity s/t %s" % activity.StartTime)

                if "is_live" in actInfo and actInfo["is_live"]:
                    exclusions.append(APIExcludeActivity("Not complete", activity_id=actInfo["id"], permanent=False, user_exception=UserException(UserExceptionType.LiveTracking)))
                    continue

                if "end_time" in actInfo:
                    activity.EndTime = self._parseDate(actInfo["end_time"])

                if actInfo["sport"] in self._activityMappings:
                    activity.Type = self._activityMappings[actInfo["sport"]]

                # "duration" is timer time
                if "duration" in actInfo:
                    activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(actInfo["duration"]))

                if "distance" in actInfo:
                    activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(actInfo["distance"]))

                if "calories" in actInfo:
                    activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(actInfo["calories"]))

                activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters)

                if "altitude_max" in actInfo:
                    activity.Stats.Elevation.Max = float(actInfo["altitude_max"])

                if "altitude_min" in actInfo:
                    activity.Stats.Elevation.Min = float(actInfo["altitude_min"])

                if "ascent" in actInfo:
                    activity.Stats.Elevation.Gain = float(actInfo["ascent"])

                if "descent" in actInfo:
                    activity.Stats.Elevation.Loss = float(actInfo["descent"])

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
                
                if "duration" in actInfo:
                    activity.EndTime = self._parseDate(actInfo["start_time"]) + timedelta(seconds=actInfo["duration"])
        
                activity.ServiceData = {"workoutId": int(actInfo["id"]), "Sport": actInfo["sport"]}

                activity.CalculateUID()
                activities.append(activity)

            if "paging" not in resp.json():
                break 
            
            paging = resp.json()["paging"]
            
            if "next" not in paging or not paging["next"] or not exhaustive:
                break

            else:
                page_query = requests.utils.urlparse(paging["next"]).query
                page_params = dict(x.split('=') for x in page_query.split('&'))

                params.update({"date_from": listStart.strftime(limitDateFormat), "date_to": listEnd.strftime(limitDateFormat), "offset": page_params.get("offset"), "limit": page_params.get("limit")})

        return activities, exclusions

    # Version that downloads and parses TCX
    def DownloadActivityTCX(self, serviceRecord, activity):
        """
        GET Activity as a PWX File

        URL: http://endoalt_proxy/workout/<WORKOUT-ID>/tcx
        Parameters:
        user = username
        pass = password

        """

        workoutId = activity.ServiceData["workoutId"]
        logger.debug("Download workout ID: " + str(workoutId))
        params = self._add_auth_params({}, record=serviceRecord)
        resp = requests.get(self._urlRoot + "/workout/{}/tcx".format(workoutId), params=params)

        if resp.status_code != 200:
          if resp.status_code == 403:
            raise APIException("No authorization to download activity with workout ID: {}".format(workoutId), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to download activity with workout ID: {}".format(workoutId))

        TCXIO.Parse(resp.content, activity)

        return activity
    
    # Version that downloads and parses GPX
    def DownloadActivityGPX(self, serviceRecord, activity):
        """
        GET Activity as a PWX File

        URL: http://endoalt_proxy/workout/<WORKOUT-ID>/tcx
        Parameters:
        user = username
        pass = password

        """

        workoutId = activity.ServiceData["workoutId"]
        logger.debug("Download workout ID: " + str(workoutId))
        params = self._add_auth_params({}, record=serviceRecord)
        resp = requests.get(self._urlRoot + "/workout/{}/tcx".format(workoutId), params=params)

        if resp.status_code != 200:
          if resp.status_code == 403:
            raise APIException("No authorization to download activity with workout ID: {}".format(workoutId), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to download activity with workout ID: {}".format(workoutId))

        GPXIO.Parse(resp.content, activity)
        
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        """
        GET Activity data

        URL: http://endoalt_proxy/workout/<WORKOUT-ID>
        Parameters:
        user = username
        pass = password

        """

        workoutId = activity.ServiceData["workoutId"]
        logger.debug("Download workout ID: " + str(workoutId))
        params = self._add_auth_params({}, record=serviceRecord)
        resp = requests.get(self._urlRoot + "/workout/{}".format(workoutId), params=params)

        if resp.status_code != 200:
          if resp.status_code == 403:
            raise APIException("No authorization to download activity with workout ID: {}".format(workoutId), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to download activity with workout ID: {}".format(workoutId))

        try:
            resp = resp.json()
            resp = resp["source"]
        except ValueError:
            self._rateLimitBailout(resp)
            res_txt = resp.text
            raise APIException("Parse failure in Endomondo activity download: %s" % resp.status_code)
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        activity.GPS = False

        old_location = None
        in_pause = False

        if "points" in resp and "points" in resp["points"]:
            for pt in resp["points"]["points"]:
                wp = Waypoint()
                if "time" not in pt:
                    # Manually-entered activities with a course attached to them have date-less waypoints
                    # It'd be nice to transfer those courses, but it's a concept few other sites support AFAIK
                    # So, ignore the points entirely
                    continue
                wp.Timestamp = self._parseDate(pt["time"])

                if ("latitude" in pt and "longitude" in pt) or "altitude" in pt:
                    wp.Location = Location()
                    if "latitude" in pt and "longitude" in pt:
                        wp.Location.Latitude = pt["latitude"]
                        wp.Location.Longitude = pt["longitude"]
                        activity.GPS = True
                    if "altitude" in pt:
                        wp.Location.Altitude = pt["altitude"]

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

                if "sensor_data" in pt:
                    if "hr" in pt["sensor_data"]:
                        wp.HR = pt["sensor_data"]["hr"]

                    if "cadence" in pt["sensor_data"]:
                        wp.Cadence = pt["sensor_data"]["cadence"]

                    if "pow" in pt["sensor_data"]:
                        wp.Power = pt["sensor_data"]["pow"]

                lap.Waypoints.append(wp)
        activity.Stationary = len(lap.Waypoints) == 0
        return activity

    # Well, this is not needed anymore ...
    # def UploadActivity(self, serviceRecord, activity):
