from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, WaypointType, Waypoint, Location, Lap
from tapiriik.settings import WEB_ROOT, MAPMYFITNESS_CLIENT_KEY, MAPMYFITNESS_CLIENT_SECRET

from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode
import json
import pytz
import requests
from django.core.urlresolvers import reverse
from requests_oauthlib import OAuth1

import logging

logger = logging.getLogger(__name__)

class MapMyFitnessService(ServiceBase):
    ID = "mapmyfitness"
    DisplayName = "MapMyFitness"
    DisplayAbbreviation = "MMR"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserAuthorizationURL = None
    AuthenticationNoFrame = True
    OutstandingOAuthRequestTokens = {}
    IsNew = True

    _activityMappings = {"16": ActivityType.Running,
                         "11": ActivityType.Cycling,
                         "41": ActivityType.MountainBiking,
                         "9": ActivityType.Walking,
                         "24": ActivityType.Hiking,
                         "398": ActivityType.DownhillSkiing,
                         "397": ActivityType.CrossCountrySkiing,  # actually "backcountry" :S
                         "107": ActivityType.Snowboarding,
                         "86": ActivityType.Skating,  # ice skating
                         "15": ActivityType.Swimming,
                         "57": ActivityType.Rowing,  # canoe/rowing
                         "211": ActivityType.Elliptical,
                         "21": ActivityType.Other}
    SupportedActivities = list(_activityMappings.values())

    def WebInit(self):
        redirect_uri = WEB_ROOT + reverse("oauth_return", kwargs={"service": "mapmyfitness"})
        params = {'client_id': MAPMYFITNESS_CLIENT_KEY,
                  'response_type': 'code',
                  'redirect_uri': redirect_uri}
        self.UserAuthorizationURL = \
            "https://api.mapmyfitness.com/v7.1/oauth2/authorize/?" + urlencode(params)

    def GenerateUserAuthorizationURL(self, session, level=None):
        oauth = OAuth1(MAPMYFITNESS_CLIENT_KEY, client_secret=MAPMYFITNESS_CLIENT_SECRET)
        response = requests.post("https://api.mapmyfitness.com/v7.1/oauth2/request_token", auth=oauth)
        credentials = parse_qs(response.text)
        token = credentials["oauth_token"][0]
        self.OutstandingOAuthRequestTokens[token] = credentials["oauth_token_secret"][0]
        reqObj = {"oauth_token": token, "oauth_callback": WEB_ROOT + reverse("oauth_return", kwargs={"service": "mapmyfitness"})}
        return "https://api.mapmyfitness.com/v7.1/oauth2/authorize?" + urlencode(reqObj)

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "Bearer " + serviceRecord.Authorization["Token"],
                "Accept-Charset": "UTF-8"}

    def _getUserId(self, serviceRecord):
        response = requests.get("https://api.mapmyfitness.com/v7.1/user/self", headers=self._apiHeaders(serviceRecord))
        responseData = response.json()
        return responseData["id"]

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service

        code = req.GET.get("code")
        params = {"grant_type": "authorization_code",
                  "code": code,
                  "client_id": MAPMYFITNESS_CLIENT_KEY,
                  "client_secret": MAPMYFITNESS_CLIENT_SECRET,
                  "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "mapmyfitness"})}

        response = requests.post("https://api.mapmyfitness.com/v7.1/oauth2/access_token",
                                 data=urlencode(params),
                                 headers={"Content-Type": "application/x-www-form-urlencoded",
                                          "api-key": MAPMYFITNESS_CLIENT_KEY})

        if response.status_code != 200:
            raise APIException("Invalid code")
        token = response.json()["access_token"]

        uid = self._getUserId(ServiceRecord({"Authorization": {"Token": token}}))

        return (uid, {"Token": token})

    def RevokeAuthorization(self, serviceRecord):
        # there doesn't seem to be a way to revoke the token
        pass

    def _getActivityTypeHierarchy(self, headers):
        if hasattr(self, "_activityTypes"):
            return self._activityTypes
        response = requests.get("https://api.mapmyfitness.com/v7.1/activity_type", headers=headers)
        data = response.json()
        self._activityTypes = {}
        for actType in data["_embedded"]["activity_types"]:
            self._activityTypes[actType["_links"]["self"][0]["id"]] = actType
        return self._activityTypes

    def _resolveActivityType(self, actType, headers):
        self._getActivityTypeHierarchy(headers)
        if actType in self._activityMappings:
            return self._activityMappings[actType]
        activity = self._activityTypes[actType]
        parentLink = activity["_links"].get("parent")
        if parentLink is not None:
            parentId = parentLink[0]["id"]
            return self._resolveActivityType(parentId, headers)
        return ActivityType.Other

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        logger.debug("DownloadActivityList")
        allItems = []
        headers=self._apiHeaders(serviceRecord)
        nextRequest = '/v7.1/workout/?user=' + str(serviceRecord.ExternalID)
        while True:
            response = requests.get("https://api.mapmyfitness.com" + nextRequest, headers=headers)
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve activity list " + str(response), serviceRecord)
            data = response.json()
            allItems += data["_embedded"]["workouts"]
            nextLink = data["_links"].get("next")
            if not exhaustive or not nextLink:
                break
            nextRequest = nextLink[0]["href"]

        activities = []
        exclusions = []
        for act in allItems:
            # TODO catch exception and add to exclusions
            activity = UploadedActivity()
            activityID = act["_links"]["self"][0]["id"]
            activity.StartTime = datetime.strptime(act["start_datetime"], "%Y-%m-%dT%H:%M:%S%z")
            activity.Notes = act["notes"] if "notes" in act else None

            # aggregate
            aggregates = act["aggregates"]
            elapsed_time_total = aggregates["elapsed_time_total"] if "elapsed_time_total" in aggregates else "0"
            activity.EndTime = activity.StartTime + timedelta(0, round(float(elapsed_time_total)))
            activity.Stats.TimerTime  = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(elapsed_time_total))
            activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(elapsed_time_total))
            if "active_time_total" in aggregates:
                activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(aggregates["active_time_total"]))

            if "distance_total" in aggregates:
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=float(aggregates["distance_total"]))

            if "speed_min" in aggregates:
                activity.Stats.Speed.Min = float(aggregates["speed_min"])
            if "speed_max" in aggregates:
                activity.Stats.Speed.Max = float(aggregates["speed_max"])
            if "speed_avg" in aggregates:
                activity.Stats.Speed.Average = float(aggregates["speed_avg"])

            if "heartrate_min" in aggregates:
                activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, min=float(aggregates["heartrate_min"])))
            if "heartrate_max" in aggregates:
                activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=float(aggregates["heartrate_max"])))
            if "heartrate_avg" in aggregates:
                activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(aggregates["heartrate_avg"]))

            if "cadence_min" in aggregates:
                activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, min=int(aggregates["cadence_min"])))
            if "cadence_max" in aggregates:
                activity.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, max=int(aggregates["cadence_max"])))
            if "cadence_avg" in aggregates:
                activity.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=int(aggregates["cadence_avg"]))

            if "power_min" in aggregates:
                activity.Stats.Power.update(ActivityStatistic(ActivityStatisticUnit.Watts, min=int(aggregates["power_min"])))
            if "power_max" in aggregates:
                activity.Stats.Power.update(ActivityStatistic(ActivityStatisticUnit.Watts, max=int(aggregates["power_max"])))
            if "power_avg" in aggregates:
                activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=int(aggregates["power_avg"]))


            activityTypeLink = act["_links"].get("activity_type")
            activityTypeID = activityTypeLink[0]["id"] if activityTypeLink is not None else None

            privacyLink = act["_links"].get("privacy")
            privacyID = privacyLink[0]["id"] if privacyLink is not None else None
            activity.Private = privacyID == "0"

            activity.Type = self._resolveActivityType(activityTypeID, headers)

            activity.ServiceData = {
                "ActivityID": activityID,
                "activityTypeID": activityTypeID,
                "privacyID": privacyID
                }
            activity.CalculateUID()
            activities.append(activity)
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        activityID = activity.ServiceData["ActivityID"]
        logger.debug("DownloadActivity %s" % activityID)

        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]
        lap.Waypoints = []

        response = requests.get("https://api.mapmyfitness.com/v7.1/workout/" + activityID + "/?field_set=time_series", headers=self._apiHeaders(serviceRecord))
        data = response.json()

        activity.GPS = False
        activity.Stationary = True

        # add waypoints to laps
        if "time_series" in data and "position" in data["time_series"]:
            activity.Stationary = False
            for pt in data["time_series"]["position"]:
                timestamp = pt[0]
                wp = Waypoint(activity.StartTime + timedelta(seconds=round(timestamp)))

                pos = pt[1]
                if ("lat" in pos and "lng" in pos) or "elevation" in pos:
                    wp.Location = Location()
                    if "lat" in pos and "lng" in pos:
                        wp.Location.Latitude = pos["lat"]
                        wp.Location.Longitude = pos["lng"]
                        activity.GPS = True
                    if "elevation" in pos:
                        wp.Location.Altitude = pos["elevation"]

                lap.Waypoints.append(wp)

        return activity

    def UploadActivity(self, serviceRecord, activity):

        if activity.Private:
            privacy_option_id = "0"
        else:
            privacy_option_id = "3"

        activity_type_id = [k for k,v in self._activityMappings.items() if v == activity.Type][0]
        if not activity_type_id:
            activity_type_id = "1"

        elapsed_time_total = activity.EndTime - activity.StartTime

        aggregates = {
            "elapsed_time_total": elapsed_time_total.seconds
        }

        if activity.Stats.Distance.Value is not None:
            aggregates["distance_total"] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value

        if activity.Stats.TimerTime.Value is not None:
            aggregates["active_time_total"] = activity.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        elif activity.Stats.MovingTime.Value is not None:
            aggregates["active_time_total"] = activity.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        else:
            aggregates["active_time_total"] = (activity.EndTime - activity.StartTime).total_seconds()

        speed_stats = activity.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond)
        if speed_stats.Average is not None:
            aggregates["speed_avg"] = speed_stats.Average
        if speed_stats.Min is not None:
            aggregates["speed_min"] = speed_stats.Min
        if speed_stats.Max is not None:
            aggregates["speed_max"] = speed_stats.Max

        hr_stats = activity.Stats.HR.asUnits(ActivityStatisticUnit.BeatsPerMinute)
        if hr_stats.Average is not None:
            aggregates["heart_rate_avg"] = hr_stats.Average
        if hr_stats.Min is not None:
            aggregates["heart_rate_max"] = hr_stats.Min
        if hr_stats.Max is not None:
            aggregates["heart_rate_max"] = hr_stats.Max

        if activity.Stats.Power.Average is not None:
            aggregates["power_avg"] = activity.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Average
        if activity.Stats.Power.Min is not None:
            aggregates["power_min"] = activity.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Min
        if activity.Stats.Power.Max is not None:
            aggregates["power_max"] = activity.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Max

        if activity.Stats.Cadence.Average is not None:
            aggregates["cadence_avg"] = activity.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Average
        elif activity.Stats.RunCadence.Average is not None:
            aggregates["cadence_avg"] = activity.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute).Average

        if activity.Stats.Cadence.Min is not None:
            aggregates["cadence_min"] = activity.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Min
        elif activity.Stats.RunCadence.Min is not None:
            aggregates["cadence_min"] = activity.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute).Min

        if activity.Stats.Cadence.Max is not None:
            aggregates["cadence_max"] = activity.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Max
        elif activity.Stats.RunCadence.Max is not None:
            aggregates["cadence_max"] = activity.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute).Max

        # time series
        position = []
        heartrate = []
        power = []
        distance = []
        speed = []
        cadence = []
        for wp in activity.GetFlatWaypoints():
            time = wp.Timestamp - activity.StartTime
            time = time.seconds
            if wp.Location:
                pos = {}
                if wp.Location.Latitude is not None and wp.Location.Longitude is not None:
                    pos["lat"] = wp.Location.Latitude
                    pos["lng"] = wp.Location.Longitude
                if wp.Location.Altitude is not None:
                    pos["elevation"] = wp.Location.Altitude
                pt = [time, pos]
                position.append(pt)
            if wp.HR is not None:
                pt = [time, round(wp.HR)]
                heartrate.append(pt)
            if wp.Power is not None:
                pt = [time, round(wp.Power)]
                power.append(pt)
            if wp.Distance is not None:
                pt = [time, round(wp.Distance)]
                distance.append(pt)
            if wp.Speed is not None:
                pt = [time, round(wp.Speed)]
                speed.append(pt)
            if wp.Cadence is not None:
                pt = [time, round(wp.Cadence)]
                cadence.append(pt)
            elif wp.RunCadence is not None:
                pt = [time, round(wp.RunCadence)]
                cadence.append(pt)

        time_series = {}
        if position:
            time_series["position"] = position
        if heartrate:
            time_series["heartrate"] = heartrate
        if power:
            time_series["power"] = power
        if distance:
            time_series["distance"] = distance
        if speed:
            time_series["speed"] = speed
        if cadence:
            time_series["cadence"] = cadence

        upload_data = {
            "start_datetime": activity.StartTime.isoformat(),
            "start_locale_timezone": activity.TZ.zone,
            "name": activity.Name,
            "privacy": "/v7.1/privacy_option/%s/" % privacy_option_id,
            "activity_type": "/v7.1/activity_type/%s/" % activity_type_id,
            "aggregates": aggregates,
            "time_series": time_series
        }

        if activity.Notes:
            upload_data["notes"] = activity.Notes

        upload_resp = requests.post(
            "https://api.mapmyfitness.com/v7.1/workout/",
             headers=self._apiHeaders(serviceRecord),
             data=json.dumps(upload_data))
        if upload_resp.status_code != 201:
            raise APIException("Could not upload activity %s %s" % (upload_resp.status_code, upload_resp.text))

        return upload_resp.json()["_links"]["self"][0]["id"]

    def DeleteCachedData(self, serviceRecord):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        pass
