import os
from datetime import datetime, timedelta
import dateutil.parser

import pytz
from dateutil.tz import tzutc
import requests
from django.core.urlresolvers import reverse

from tapiriik.settings import WEB_ROOT, RWGPS_APIKEY
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.fit import FITIO
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

import logging
logger = logging.getLogger(__name__)

class RideWithGPSService(ServiceBase):
    ID = "rwgps"
    DisplayName = "Ride With GPS"
    DisplayAbbreviation = "RWG"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    # RWGPS does has a "recreation_types" list, but it is not actually used anywhere (yet)
    # (This is a subset of the things returned by that list for future reference...)
    _activityMappings = {
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain biking": ActivityType.MountainBiking,
                                "Hiking": ActivityType.Hiking,
                                "all": ActivityType.Other  # everything will eventually resolve to this
    }

    SupportedActivities = [ActivityType.Cycling, ActivityType.MountainBiking]

    SupportsHR = SupportsCadence = True

    _sessionCache = SessionCache("rwgps", lifetime=timedelta(minutes=30), freshen_on_get=True)

    def _add_auth_params(self, params=None, record=None):
        """
        Adds apikey and authorization (email/password) to the passed-in params,
        returns modified params dict.
        """
        from tapiriik.auth.credential_storage import CredentialStore
        if params is None:
            params = {}
        params['apikey'] = RWGPS_APIKEY
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
            params['email'] = email
            params['password'] = password
        return params

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        res = requests.get("https://ridewithgps.com/users/current.json",
                           params={'email': email, 'password': password, 'apikey': RWGPS_APIKEY})
        if res.status_code == 401:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        res.raise_for_status()
        res = res.json()
        if res["user"] is None:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        member_id = res["user"]["id"]
        if not member_id:
            raise APIException("Unable to retrieve id", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (member_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def _duration_to_seconds(self, s):
        """
        Converts a duration in form HH:MM:SS to number of seconds for use in timedelta construction.
        """
        hours, minutes, seconds = (["0", "0"] + str(s).split(":"))[-3:]
        hours = int(hours)
        minutes = int(minutes)
        seconds = float(seconds)
        total_seconds = int(hours + 60000 * minutes + 1000 * seconds)
        return total_seconds

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        def mapStatTriple(act, stats_obj, key, units):
            if "%s_max" % key in act and act["%s_max" % key]:
                stats_obj.update(ActivityStatistic(units, max=float(act["%s_max" % key])))
            if "%s_min" % key in act and act["%s_min" % key]:
                stats_obj.update(ActivityStatistic(units, min=float(act["%s_min" % key])))
            if "%s_avg" % key in act and act["%s_avg" % key]:
                stats_obj.update(ActivityStatistic(units, avg=float(act["%s_avg" % key])))


        # http://ridewithgps.com/users/1/trips.json?limit=200&order_by=created_at&order_dir=asc
        # offset also supported
        activities = []
        exclusions = []
        # They don't actually support paging right now, for whatever reason
        params = self._add_auth_params({}, record=serviceRecord)

        res = requests.get("https://ridewithgps.com/users/{}/trips.json".format(serviceRecord.ExternalID), params=params)
        res = res.json()

        # Apparently some API users are seeing this new result format - I'm not
        if type(res) is dict:
            res = res.get("results", [])

        if res == []:
            return [], [] # No activities
        for act in res:
            if "distance" not in act:
                exclusions.append(APIExcludeActivity("No distance", activity_id=act["id"], user_exception=UserException(UserExceptionType.Corrupt)))
                continue
            if "duration" not in act or not act["duration"]:
                exclusions.append(APIExcludeActivity("No duration", activity_id=act["id"], user_exception=UserException(UserExceptionType.Corrupt)))
                continue
            activity = UploadedActivity()

            logger.debug("Name " + act["name"] + ":")
            if act.get("name", None) and len(act["name"].strip()):
                activity.Name = act["name"]

            if act.get("description", None) and len(act["description"].strip()):
                activity.Notes = act["description"]

            activity.GPS = act["is_gps"]
            activity.Stationary = not activity.GPS # I think

            # 0 = public, 1 = private, 2 = friends
            activity.Private = act["visibility"] == 1

            activity.StartTime = dateutil.parser.parse(act["departed_at"])

            try:
                activity.TZ = pytz.timezone(act["time_zone"])
            except pytz.exceptions.UnknownTimeZoneError:
                # Sometimes the time_zone returned isn't quite what we'd like it
                # So, just pull the offset from the datetime
                if isinstance(activity.StartTime.tzinfo, tzutc):
                    activity.TZ = pytz.utc # The dateutil tzutc doesn't have an _offset value.
                else:
                    activity.TZ = pytz.FixedOffset(activity.StartTime.tzinfo.utcoffset(activity.StartTime).total_seconds() / 60)

            activity.StartTime = activity.StartTime.replace(tzinfo=activity.TZ) # Overwrite dateutil's sillyness

            activity.EndTime = activity.StartTime + timedelta(seconds=self._duration_to_seconds(act["duration"]))
            logger.debug("Activity s/t " + str(activity.StartTime))
            activity.AdjustTZ()

            activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, float(act["distance"]))

            mapStatTriple(act, activity.Stats.Power, "watts", ActivityStatisticUnit.Watts)
            mapStatTriple(act, activity.Stats.Speed, "speed", ActivityStatisticUnit.KilometersPerHour)
            mapStatTriple(act, activity.Stats.Cadence, "cad", ActivityStatisticUnit.RevolutionsPerMinute)
            mapStatTriple(act, activity.Stats.HR, "hr", ActivityStatisticUnit.BeatsPerMinute)

            if "elevation_gain" in act and act["elevation_gain"]:
                activity.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(act["elevation_gain"])))

            if "elevation_loss" in act and act["elevation_loss"]:
                activity.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, loss=float(act["elevation_loss"])))

            # Activity type is not implemented yet in RWGPS results; we will assume cycling, though perhaps "OTHER" wouuld be correct
            activity.Type = ActivityType.Cycling

            activity.CalculateUID()
            activity.ServiceData = {"ActivityID": act["id"]}
            activities.append(activity)
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        if activity.Stationary:
            return activity # Nothing more to download - it doesn't serve these files for manually entered activites
        # https://ridewithgps.com/trips/??????.tcx
        activityID = activity.ServiceData["ActivityID"]
        res = requests.get("https://ridewithgps.com/trips/{}.tcx".format(activityID),
                           params=self._add_auth_params({'sub_format': 'history'}, record=serviceRecord))
        try:
            TCXIO.Parse(res.content, activity)
        except ValueError as e:
            raise APIExcludeActivity("TCX parse error " + str(e), user_exception=UserException(UserExceptionType.Corrupt))

        return activity

    def UploadActivity(self, serviceRecord, activity):
        # https://ridewithgps.com/trips.json

        fit_file = FITIO.Dump(activity)
        files = {"data_file": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", fit_file)}
        params = {}
        params['trip[name]'] = activity.Name
        params['trip[description]'] = activity.Notes
        if activity.Private:
            params['trip[visibility]'] = 1 # Yes, this logic seems backwards but it's how it works

        res = requests.post("https://ridewithgps.com/trips.json", files=files,
                            params=self._add_auth_params(params, record=serviceRecord))
        if res.status_code % 100 == 4:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        res.raise_for_status()
        res = res.json()
        if res["success"] != 1:
            raise APIException("Unable to upload activity")


    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
