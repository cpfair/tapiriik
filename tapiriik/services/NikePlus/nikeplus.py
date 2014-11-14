from datetime import datetime, timedelta
import dateutil.parser
from dateutil.tz import tzutc
import requests
import json
import calendar
import pytz
import os

from django.core.urlresolvers import reverse

from tapiriik.settings import WEB_ROOT, NIKEPLUS_CLIENT_ID, NIKEPLUS_CLIENT_SECRET, NIKEPLUS_CLIENT_NAME
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, Lap, WaypointType, Location, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.fit import FITIO
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache
from tapiriik.services.stream_sampling import StreamSampler

import logging
logger = logging.getLogger(__name__)

class NikePlusService(ServiceBase):
    ID = "nikeplus"
    DisplayName = "Nike+"
    DisplayAbbreviation = "N+"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesStationaryActivities = False # No manual entry afaik

    _activityMappings = {
        "RUN": ActivityType.Running,
        "JOGGING": ActivityType.Running,
        "WALK": ActivityType.Walking,
        "CYCLE": ActivityType.Cycling,
        "STATIONARY_BIKING": ActivityType.Cycling,
        "MOUNTAIN_BIKING": ActivityType.MountainBiking,
        "CROSS_COUNTRY": ActivityType.CrossCountrySkiing, # Well, I think?
        "ELLIPTICAL": ActivityType.Elliptical,
        "HIKING": ActivityType.Hiking,
        "ROCK_CLIMBING": ActivityType.Climbing,
        "ICE_CLIMBING": ActivityType.Climbing,
        "SNOWBOARDING": ActivityType.Snowboarding,
        "SKIING": ActivityType.DownhillSkiing,
        "ICE_SKATING": ActivityType.Skating,
        "OTHER": ActivityType.Other
    }

    # Leave it to Nike+ to invent new timezones
    _timezones = {
        "ART": "America/Argentina/Buenos_Aires" # Close enough
    }

    _reverseActivityMappings = {
        "RUN": ActivityType.Running,
        # Their web frontend has a meltdown even trying to navigate to other activity types, who knows
        # So I won't exacerbate the problem...
        # "WALK": ActivityType.Walking,
        # "CYCLE": ActivityType.Cycling,
        # "MOUNTAIN_BIKING": ActivityType.MountainBiking,
        # "CROSS_COUNTRY": ActivityType.CrossCountrySkiing,
        # "ELLIPTICAL": ActivityType.Elliptical,
        # "HIKING": ActivityType.Hiking,
        # "ROCK_CLIMBING": ActivityType.Climbing,
        # "SNOWBOARDING": ActivityType.Snowboarding,
        # "SKIING": ActivityType.DownhillSkiing,
        # "ICE_SKATING": ActivityType.Skating,
        # "OTHER": ActivityType.Other
    }

    SupportedActivities = list(_reverseActivityMappings.values())

    _sessionCache = SessionCache(lifetime=timedelta(minutes=45), freshen_on_get=False)

    _obligatoryHeaders = {
        "User-Agent": "NPConnect",
        "appId": NIKEPLUS_CLIENT_NAME
    }

    _obligatoryCookies = {
        "app": NIKEPLUS_CLIENT_NAME,
        "client_id": NIKEPLUS_CLIENT_ID,
        "client_secret": NIKEPLUS_CLIENT_SECRET
    }

    def _get_session(self, record=None, email=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        cached = self._sessionCache.Get(record.ExternalID if record else email)
        if cached and not skip_cache:
            return cached
        if record:
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        # This is the most pleasent login flow I've dealt with in a long time
        session = requests.Session()
        session.headers.update(self._obligatoryHeaders)
        session.cookies.update(self._obligatoryCookies)

        res = session.post(
            "https://api.nike.com/nsl/user/login",
            params={"format": "json",  "app": "app", "client_id": NIKEPLUS_CLIENT_ID, "client_secret": NIKEPLUS_CLIENT_SECRET},
            data={"email": email, "password": password},
            headers={"Accept": "application/json"}
        )

        if res.status_code >= 500 and res.status_code < 600:
            raise APIException("Login exception %s - %s" % (res.status_code, res.text))

        res_obj = res.json()

        if "access_token" not in res_obj:
            raise APIException("Invalid login %s - %s / %s" % (res.status_code, res.text, res.cookies), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        # Was getting a super obscure error from the nether regions of requestse about duplicate cookies
        # So, store this in an easier-to-find location
        session.access_token = res_obj["access_token"]

        self._sessionCache.Set(record.ExternalID if record else email, session)

        return session

    def _with_auth(self, session, params={}):
        # For whatever reason the access_token needs to be a GET parameter :(
        params.update({"access_token": session.access_token, "app": NIKEPLUS_CLIENT_NAME})
        return params

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(email=email, password=password)

        user_data = session.get("https://api.nike.com/nsl/user/get", params=self._with_auth(session, {"format": "json"}))
        user_id = int(user_data.json()["serviceResponse"]["body"]["User"]["id"])

        return (user_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def _durationToTimespan(self, duration):
        # Hours:Minutes:Seconds.Milliseconds
        duration = [float(x) for x in duration.split(":")]
        return timedelta(seconds=duration[2], minutes=duration[1], hours=duration[0])

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._get_session(serviceRecord)
        list_params = self._with_auth(session, {"count": 20, "offset": 1})

        activities = []
        exclusions = []

        while True:
            list_resp = session.get("https://api.nike.com/me/sport/activities", params=list_params)
            list_resp = list_resp.json()

            for act in list_resp["data"]:
                activity = UploadedActivity()
                activity.ServiceData = {"ID": act["activityId"]}

                if act["status"] != "COMPLETE":
                    exclusions.append(APIExcludeActivity("Not complete", activityId=act["activityId"], permanent=False, user_exception=UserException(UserExceptionType.LiveTracking)))
                    continue

                activity.StartTime = dateutil.parser.parse(act["startTime"]).replace(tzinfo=pytz.utc)
                activity.EndTime = activity.StartTime + self._durationToTimespan(act["metricSummary"]["duration"])

                tz_name = act["activityTimeZone"]

                # They say these are all IANA standard names - they aren't
                if tz_name in self._timezones:
                    tz_name = self._timezones[tz_name]

                activity.TZ = pytz.timezone(tz_name)

                if act["activityType"] in self._activityMappings:
                    activity.Type = self._activityMappings[act["activityType"]]

                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(act["metricSummary"]["distance"]))
                activity.Stats.Strides = ActivityStatistic(ActivityStatisticUnit.Strides, value=int(act["metricSummary"]["steps"]))
                activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(act["metricSummary"]["calories"]))
                activity.CalculateUID()
                activities.append(activity)

            if len(list_resp["data"]) == 0 or not exhaustive:
                break
            list_params["offset"] += list_params["count"]

        return activities, exclusions

    def _nikeStream(self, stream, values_collection="values"):
        if stream["intervalUnit"] != "SEC":
            # Who knows if they ever return it in a different unit? Their docs don't give a list
            raise Exception("Unknown stream interval unit %s" % stream["intervalUnit"])

        interval = timedelta(seconds=stream["intervalMetric"]).total_seconds()
        for x in range(len(stream[values_collection])):
            yield (interval * x, stream[values_collection][x])

    def DownloadActivity(self, serviceRecord, activity):
        session = self._get_session(serviceRecord)
        act_id = activity.ServiceData["ID"]
        activityDetails = session.get("https://api.nike.com/me/sport/activities/%s" % act_id, params=self._with_auth(session))
        activityDetails = activityDetails.json()

        streams = {metric["metricType"].lower(): self._nikeStream(metric) for metric in activityDetails["metrics"]}

        activity.GPS = activityDetails["isGpsActivity"]

        if activity.GPS:
            activityGps = session.get("https://api.nike.com/me/sport/activities/%s/gps" % act_id, params=self._with_auth(session))
            activityGps = activityGps.json()
            streams["gps"] = self._nikeStream(activityGps, "waypoints")
            activity.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters,
                                                              gain=float(activityGps["elevationGain"]),
                                                              loss=float(activityGps["elevationLoss"]),
                                                              max=float(activityGps["elevationMax"]),
                                                              min=float(activityGps["elevationMin"])))

        lap = Lap(startTime=activity.StartTime, endTime=activity.EndTime)
        lap.Stats = activity.Stats
        activity.Laps = [lap]
        # I thought I wrote StreamSampler to be generator-friendly - nope.
        streams = {k: list(v) for k,v in streams.items()}

        # The docs are unclear on which of these are actually stream metrics, oh well
        def stream_waypoint(offset, speed=None, distance=None, heartrate=None, calories=None, steps=None, watts=None, gps=None, **kwargs):
            wp = Waypoint()
            wp.Timestamp = activity.StartTime + timedelta(seconds=offset)
            wp.Speed = float(speed) if speed else None
            wp.Distance = float(distance) / 1000 if distance else None
            wp.HR = float(heartrate) if heartrate else None
            wp.Calories = float(calories) if calories else None
            wp.Power = float(watts) if watts else None

            if gps:
                wp.Location = Location(lat=float(gps["latitude"]), lon=float(gps["longitude"]), alt=float(gps["elevation"]))
            lap.Waypoints.append(wp)

        StreamSampler.SampleWithCallback(stream_waypoint, streams)

        activity.Stationary = len(lap.Waypoints) == 0

        return activity

    def UploadActivity(self, serviceRecord, activity):
        metrics = {
            "data": [],
            "metricTypes": [],
            "intervalUnit": "SEC",
            "intervalValue": 10 if activity.Type == ActivityType.Running else 5 # What a joke.
        }

        act = [{
            "deviceName": "tapiriik",
            "deviceType": "BIKE" if activity.Type == ActivityType.Cycling else "WATCH", # ??? nike+ is weird
            "startTime": calendar.timegm(activity.StartTime.astimezone(pytz.utc).timetuple()) * 1000,
            "timeZoneName": str(activity.TZ),
            "activityType": [k for k,v in self._reverseActivityMappings.items() if v == activity.Type][0],
            "metrics": metrics
        }]

        wps = activity.GetFlatWaypoints()
        wpidx = 0
        full_metrics = []
        max_metrics = set()
        for offset in range(0, int((activity.EndTime - activity.StartTime).total_seconds()), metrics["intervalValue"]):
            # Pick the most recent waypoint in the past
            while len(wps) > wpidx + 1 and (wps[wpidx + 1].Timestamp - activity.StartTime).total_seconds() < offset:
                wpidx += 1
            wp = wps[wpidx]
            my_metrics = {}

            if wp.Location and wp.Location.Latitude is not None and wp.Location.Longitude is not None:
                elev = wp.Location.Altitude if wp.Location.Altitude else 0 # They always require this field, it's meh
                my_metrics.update({"latitude": wp.Location.Latitude, "longitude": wp.Location.Longitude, "elevation": elev})

            if wp.Distance is not None:
                my_metrics["distance"] = wp.Distance / 1000 # m -> km

            if wp.HR is not None:
                my_metrics["heartrate"] = round(wp.HR)

            if wp.Speed is not None:
                my_metrics["speed"] = wp.Speed

            if wp.Calories is not None:
                my_metrics["calories"] = round(wp.Calories)

            if wp.Power is not None:
                my_metrics["watts"] = round(wp.Power)

            max_metrics |= my_metrics.keys()
            full_metrics.append(my_metrics)

        max_metrics = sorted(list(max_metrics))
        metrics["metricTypes"] = max_metrics

        # Passing null metric values makes Nike+ sad
        # So we hold the last value until a new one is available
        frame_hold = {x: 0 for x in max_metrics} # Blegh, close enough
        for metric_frame in full_metrics:
            frame_hold.update(metric_frame)
            metrics["data"].append([frame_hold[x] for x in max_metrics])

        headers = {
            "Content-Type": "application/json"
        }

        session = self._get_session(serviceRecord)
        upload_resp = session.post("https://api.nike.com/me/sport/activities", params=self._with_auth(session), data=json.dumps(act), headers=headers)

        if upload_resp.status_code != 201:
            error_codes = [x["code"] for x in upload_resp.json()["errors"]]
            if 320 in error_codes: # Invalid combination of metric types and blah blah blah
                raise APIException("Not enough data, have keys %s" % max_metrics, user_exception=UserException(UserExceptionType.InsufficientData))
            raise APIException("Could not upload activity %s - %s" % (upload_resp.status_code, upload_resp.text))

        return upload_resp.json()[0]["activityId"]

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
