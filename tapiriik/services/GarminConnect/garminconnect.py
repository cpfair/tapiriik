from tapiriik.settings import WEB_ROOT, HTTP_SOURCE_ADDR, GARMIN_CONNECT_USER_WATCH_ACCOUNTS
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, Location, Lap
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.statistic_calculator import ActivityStatisticCalculator
from tapiriik.services.tcx import TCXIO
from tapiriik.services.gpx import GPXIO
from tapiriik.services.fit import FITIO
from tapiriik.services.sessioncache import SessionCache
from tapiriik.services.devices import DeviceIdentifier, DeviceIdentifierType, Device
from tapiriik.database import cachedb, db

from django.core.urlresolvers import reverse
import pytz
from datetime import datetime, timedelta
import requests
import os
import math
import logging
import time
import json
import re
import random
import tempfile
import json
from urllib.parse import urlencode
logger = logging.getLogger(__name__)

class GarminConnectService(ServiceBase):
    ID = "garminconnect"
    DisplayName = "Garmin Connect"
    DisplayAbbreviation = "GC"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    PartialSyncRequiresTrigger = len(GARMIN_CONNECT_USER_WATCH_ACCOUNTS) > 0
    PartialSyncTriggerPollInterval = timedelta(minutes=20)
    PartialSyncTriggerPollMultiple = len(GARMIN_CONNECT_USER_WATCH_ACCOUNTS.keys())

    ConfigurationDefaults = {
        "WatchUserKey": None,
        "WatchUserLastID": 0
    }

    _activityMappings = {
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain_biking": ActivityType.MountainBiking,
                                "walking": ActivityType.Walking,
                                "hiking": ActivityType.Hiking,
                                "resort_skiing_snowboarding": ActivityType.DownhillSkiing,
                                "cross_country_skiing": ActivityType.CrossCountrySkiing,
                                "skate_skiing": ActivityType.CrossCountrySkiing, # Well, it ain't downhill?
                                "backcountry_skiing_snowboarding": ActivityType.CrossCountrySkiing,  # ish
                                "skating": ActivityType.Skating,
                                "swimming": ActivityType.Swimming,
                                "rowing": ActivityType.Rowing,
                                "elliptical": ActivityType.Elliptical,
                                "fitness_equipment": ActivityType.Gym,
                                "rock_climbing": ActivityType.Climbing,
                                "mountaineering": ActivityType.Climbing,
                                "all": ActivityType.Other,  # everything will eventually resolve to this
                                "multi_sport": ActivityType.Other # Most useless type? You decide!
    }

    _reverseActivityMappings = {  # Removes ambiguities when mapping back to their activity types
                                "running": ActivityType.Running,
                                "cycling": ActivityType.Cycling,
                                "mountain_biking": ActivityType.MountainBiking,
                                "walking": ActivityType.Walking,
                                "hiking": ActivityType.Hiking,
                                "resort_skiing_snowboarding": ActivityType.DownhillSkiing,
                                "cross_country_skiing": ActivityType.CrossCountrySkiing,
                                "skating": ActivityType.Skating,
                                "swimming": ActivityType.Swimming,
                                "rowing": ActivityType.Rowing,
                                "elliptical": ActivityType.Elliptical,
                                "fitness_equipment": ActivityType.Gym,
                                "rock_climbing": ActivityType.Climbing,
                                "other": ActivityType.Other  # I guess? (vs. "all" that is)
    }

    SupportedActivities = list(_activityMappings.values())

    SupportsHR = SupportsCadence = True

    SupportsActivityDeletion = True

    _sessionCache = SessionCache("garminconnect", lifetime=timedelta(minutes=120), freshen_on_get=True)
    _reauthAttempts = 1 # per request

    _unitMap = {
        "mph": ActivityStatisticUnit.MilesPerHour,
        "kph": ActivityStatisticUnit.KilometersPerHour,
        "hmph": ActivityStatisticUnit.HectometersPerHour,
        "hydph": ActivityStatisticUnit.HundredYardsPerHour,
        "celcius": ActivityStatisticUnit.DegreesCelcius,
        "fahrenheit": ActivityStatisticUnit.DegreesFahrenheit,
        "mile": ActivityStatisticUnit.Miles,
        "kilometer": ActivityStatisticUnit.Kilometers,
        "foot": ActivityStatisticUnit.Feet,
        "meter": ActivityStatisticUnit.Meters,
        "yard": ActivityStatisticUnit.Yards,
        "kilocalorie": ActivityStatisticUnit.Kilocalories,
        "bpm": ActivityStatisticUnit.BeatsPerMinute,
        "stepsPerMinute": ActivityStatisticUnit.DoubledStepsPerMinute,
        "rpm": ActivityStatisticUnit.RevolutionsPerMinute,
        "watt": ActivityStatisticUnit.Watts,
        "second": ActivityStatisticUnit.Seconds,
        "ms": ActivityStatisticUnit.Milliseconds
    }

    _obligatory_headers = {
        "Referer": "https://sync.tapiriik.com"
    }

    def __init__(self):
        cachedHierarchy = cachedb.gc_type_hierarchy.find_one()
        if not cachedHierarchy:
            rawHierarchy = requests.get("https://connect.garmin.com/proxy/activity-service-1.2/json/activity_types", headers=self._obligatory_headers).text
            self._activityHierarchy = json.loads(rawHierarchy)["dictionary"]
            cachedb.gc_type_hierarchy.insert({"Hierarchy": rawHierarchy})
        else:
            self._activityHierarchy = json.loads(cachedHierarchy["Hierarchy"])["dictionary"]
        rate_lock_path = tempfile.gettempdir() + "/gc_rate.%s.lock" % HTTP_SOURCE_ADDR
        # Ensure the rate lock file exists (...the easy way)
        open(rate_lock_path, "a").close()
        self._rate_lock = open(rate_lock_path, "r+")

    def _rate_limit(self):
        import fcntl, struct, time
        min_period = 1  # I appear to been banned from Garmin Connect while determining this.
        fcntl.flock(self._rate_lock,fcntl.LOCK_EX)
        try:
            self._rate_lock.seek(0)
            last_req_start = self._rate_lock.read()
            if not last_req_start:
                last_req_start = 0
            else:
                last_req_start = float(last_req_start)

            wait_time = max(0, min_period - (time.time() - last_req_start))
            time.sleep(wait_time)

            self._rate_lock.seek(0)
            self._rate_lock.write(str(time.time()))
            self._rate_lock.flush()
        finally:
            fcntl.flock(self._rate_lock,fcntl.LOCK_UN)

    def _request_with_reauth(self, serviceRecord, req_lambda):
        for i in range(self._reauthAttempts + 1):
            session = self._get_session(record=serviceRecord, skip_cache=i > 0)
            self._rate_limit()
            result = req_lambda(session)
            if result.status_code not in (403, 500):
                return result
        # Pass the failed response back any ways - another handler will catch it and provide a nicer error
        return result

    def _get_session(self, record=None, email=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        cached = self._sessionCache.Get(record.ExternalID if record else email)
        if cached and not skip_cache:
                logger.debug("Using cached credential")
                return cached
        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()

        # JSIG CAS, cool I guess.
        # Not quite OAuth though, so I'll continue to collect raw credentials.
        # Commented stuff left in case this ever breaks because of missing parameters...
        data = {
            "username": email,
            "password": password,
            "_eventId": "submit",
            "embed": "true",
            # "displayNameRequired": "false"
        }
        params = {
            "service": "https://connect.garmin.com/post-auth/login",
            # "redirectAfterAccountLoginUrl": "http://connect.garmin.com/post-auth/login",
            # "redirectAfterAccountCreationUrl": "http://connect.garmin.com/post-auth/login",
            # "webhost": "olaxpw-connect00.garmin.com",
            "clientId": "GarminConnect",
            # "gauthHost": "https://sso.garmin.com/sso",
            # "rememberMeShown": "true",
            # "rememberMeChecked": "false",
            "consumeServiceTicket": "false",
            # "id": "gauth-widget",
            # "embedWidget": "false",
            # "cssUrl": "https://static.garmincdn.com/com.garmin.connect/ui/src-css/gauth-custom.css",
            # "source": "http://connect.garmin.com/en-US/signin",
            # "createAccountShown": "true",
            # "openCreateAccount": "false",
            # "usernameShown": "true",
            # "displayNameShown": "false",
            # "initialFocus": "true",
            # "locale": "en"
        }
        # I may never understand what motivates people to mangle a perfectly good protocol like HTTP in the ways they do...
        preResp = session.get("https://sso.garmin.com/sso/login", params=params)
        if preResp.status_code != 200:
            raise APIException("SSO prestart error %s %s" % (preResp.status_code, preResp.text))
        data["lt"] = re.search("name=\"lt\"\s+value=\"([^\"]+)\"", preResp.text).groups(1)[0]

        ssoResp = session.post("https://sso.garmin.com/sso/login", params=params, data=data, allow_redirects=False)
        if ssoResp.status_code != 200 or "temporarily unavailable" in ssoResp.text:
            raise APIException("SSO error %s %s" % (ssoResp.status_code, ssoResp.text))

        if "renewPassword" in ssoResp.text:
            raise APIException("Reset password", block=True, user_exception=UserException(UserExceptionType.RenewPassword, intervention_required=True))
        ticket_match = re.search("ticket=([^']+)'", ssoResp.text)
        if not ticket_match:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        ticket = ticket_match.groups(1)[0]

        # ...AND WE'RE NOT DONE YET!

        self._rate_limit()
        gcRedeemResp = session.get("https://connect.garmin.com/post-auth/login", params={"ticket": ticket}, allow_redirects=False)
        if gcRedeemResp.status_code != 302:
            raise APIException("GC redeem-start error %s %s" % (gcRedeemResp.status_code, gcRedeemResp.text))

        # There are 6 redirects that need to be followed to get the correct cookie
        # ... :(
        expected_redirect_count = 6
        current_redirect_count = 1
        while True:
            self._rate_limit()
            gcRedeemResp = session.get(gcRedeemResp.headers["location"], allow_redirects=False)

            if current_redirect_count >= expected_redirect_count and gcRedeemResp.status_code != 200:
                raise APIException("GC redeem %d/%d error %s %s" % (current_redirect_count, expected_redirect_count, gcRedeemResp.status_code, gcRedeemResp.text))
            if gcRedeemResp.status_code == 200 or gcRedeemResp.status_code == 404:
                break
            current_redirect_count += 1
            if current_redirect_count > expected_redirect_count:
                break

        self._sessionCache.Set(record.ExternalID if record else email, session)

        session.headers.update(self._obligatory_headers)

        return session

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(email=email, password=password, skip_cache=True)
        # TODO: http://connect.garmin.com/proxy/userprofile-service/socialProfile/ has the proper immutable user ID, not that anyone ever changes this one...
        self._rate_limit()
        username = session.get("http://connect.garmin.com/user/username").json()["username"]
        if not len(username):
            raise APIException("Unable to retrieve username", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (username, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def UserUploadedActivityURL(self, uploadId):
        return "https://connect.garmin.com/modern/activity/%d" % uploadId

    def _resolveActivityType(self, act_type):
        # Mostly there are two levels of a hierarchy, so we don't really need this as the parent is included in the listing.
        # But maybe they'll change that some day?
        while act_type not in self._activityMappings:
            try:
                act_type = [x["parent"]["key"] for x in self._activityHierarchy if x["key"] == act_type][0]
            except IndexError:
                raise ValueError("Activity type not found in activity hierarchy")
        return self._activityMappings[act_type]

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        #http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities?&start=0&limit=50
        page = 1
        pageSz = 100
        activities = []
        exclusions = []
        while True:
            logger.debug("Req with " + str({"start": (page - 1) * pageSz, "limit": pageSz}))

            res = self._request_with_reauth(serviceRecord, lambda session: session.get("https://connect.garmin.com/modern/proxy/activity-search-service-1.0/json/activities", params={"start": (page - 1) * pageSz, "limit": pageSz}))

            try:
                res = res.json()["results"]
            except ValueError:
                res_txt = res.text # So it can capture in the log message
                raise APIException("Parse failure in GC list resp: %s - %s" % (res.status_code, res.text))
            if "activities" not in res:
                break  # No activities on this page - empty account.
            for act in res["activities"]:
                act = act["activity"]
                activity = UploadedActivity()

                # Don't really know why sumSampleCountTimestamp doesn't appear in swim activities - they're definitely timestamped...
                activity.Stationary = "sumSampleCountSpeed" not in act and "sumSampleCountTimestamp" not in act
                activity.GPS = "endLatitude" in act

                activity.Private = act["privacy"]["key"] == "private"

                try:
                    activity.TZ = pytz.timezone(act["activityTimeZone"]["key"])
                except pytz.exceptions.UnknownTimeZoneError:
                    activity.TZ = pytz.FixedOffset(float(act["activityTimeZone"]["offset"]) * 60)

                logger.debug("Name " + act["activityName"]["value"] + ":")
                if len(act["activityName"]["value"].strip()) and act["activityName"]["value"] != "Untitled": # This doesn't work for internationalized accounts, oh well.
                    activity.Name = act["activityName"]["value"]

                if len(act["activityDescription"]["value"].strip()):
                    activity.Notes = act["activityDescription"]["value"]

                # beginTimestamp/endTimestamp is in UTC
                activity.StartTime = pytz.utc.localize(datetime.utcfromtimestamp(float(act["beginTimestamp"]["millis"])/1000))
                if "sumElapsedDuration" in act:
                    activity.EndTime = activity.StartTime + timedelta(0, round(float(act["sumElapsedDuration"]["value"])))
                elif "sumDuration" in act:
                    activity.EndTime = activity.StartTime + timedelta(minutes=float(act["sumDuration"]["minutesSeconds"].split(":")[0]), seconds=float(act["sumDuration"]["minutesSeconds"].split(":")[1]))
                else:
                    activity.EndTime = pytz.utc.localize(datetime.utcfromtimestamp(float(act["endTimestamp"]["millis"])/1000))
                logger.debug("Activity s/t " + str(activity.StartTime) + " on page " + str(page))
                activity.AdjustTZ()

                if "sumDistance" in act and float(act["sumDistance"]["value"]) != 0:
                    activity.Stats.Distance = ActivityStatistic(self._unitMap[act["sumDistance"]["uom"]], value=float(act["sumDistance"]["value"]))

                if "device" in act and act["device"]["key"] != "unknown":
                    devId = DeviceIdentifier.FindMatchingIdentifierOfType(DeviceIdentifierType.GC, {"Key": act["device"]["key"]})
                    ver_split = act["device"]["key"].split(".")
                    ver_maj = None
                    ver_min = None
                    if len(ver_split) == 4:
                        # 2.90.0.0
                        ver_maj = int(ver_split[0])
                        ver_min = int(ver_split[1])
                    activity.Device = Device(devId, verMaj=ver_maj, verMin=ver_min)

                activity.Type = self._resolveActivityType(act["activityType"]["key"])

                activity.CalculateUID()

                activity.ServiceData = {"ActivityID": int(act["activityId"])}

                activities.append(activity)
            logger.debug("Finished page " + str(page) + " of " + str(res["search"]["totalPages"]))
            if not exhaustive or int(res["search"]["totalPages"]) == page:
                break
            else:
                page += 1
        return activities, exclusions

    def _downloadActivitySummary(self, serviceRecord, activity):
        activityID = activity.ServiceData["ActivityID"]

        summary_resp = self._request_with_reauth(serviceRecord, lambda session: session.get("https://connect.garmin.com/modern/proxy/activity-service/activity/" + str(activityID)))

        try:
            summary_data = summary_resp.json()
        except ValueError:
            raise APIException("Failure downloading activity summary %s:%s" % (summary_resp.status_code, summary_resp.text))
        stat_map = {}


        def mapStat(gcKey, statKey, type, units):
            stat_map[gcKey] = {
                "key": statKey,
                "attr": type,
                "units": units
            }

        def applyStats(gc_dict, stats_obj):
            for gc_key, stat in stat_map.items():
                if gc_key in gc_dict:
                    value = float(gc_dict[gc_key])
                    if math.isinf(value):
                        continue # GC returns the minimum speed as "-Infinity" instead of 0 some times :S
                    getattr(stats_obj, stat["key"]).update(ActivityStatistic(stat["units"], **({stat["attr"]: value})))

        mapStat("movingDuration", "MovingTime", "value", ActivityStatisticUnit.Seconds)
        mapStat("duration", "TimerTime", "value", ActivityStatisticUnit.Seconds)
        mapStat("distance", "Distance", "value", ActivityStatisticUnit.Meters)
        mapStat("maxSpeed", "Speed", "max", ActivityStatisticUnit.MetersPerSecond)
        mapStat("averageSpeed", "Speed", "avg", ActivityStatisticUnit.MetersPerSecond)
        mapStat("calories", "Energy", "value", ActivityStatisticUnit.Kilocalories)
        mapStat("maxHR", "HR", "max", ActivityStatisticUnit.BeatsPerMinute)
        mapStat("averageHR", "HR", "avg", ActivityStatisticUnit.BeatsPerMinute)
        mapStat("minElevation", "Elevation", "min", ActivityStatisticUnit.Meters)
        mapStat("maxElevation", "Elevation", "max", ActivityStatisticUnit.Meters)
        mapStat("elevationGain", "Elevation", "gain", ActivityStatisticUnit.Meters)
        mapStat("elevationLoss", "Elevation", "loss", ActivityStatisticUnit.Meters)
        mapStat("averageBikeCadence", "Cadence", "avg", ActivityStatisticUnit.RevolutionsPerMinute)
        mapStat("averageCadence", "Cadence", "avg", ActivityStatisticUnit.StepsPerMinute)

        applyStats(summary_data["summaryDTO"], activity.Stats)

        laps_resp = self._request_with_reauth(serviceRecord, lambda session: session.get("https://connect.garmin.com/modern/proxy/activity-service/activity/%s/splits" % str(activityID)))
        try:
            laps_data = laps_resp.json()
        except ValueError:
            raise APIException("Failure downloading activity laps summary %s:%s" % (laps_resp.status_code, laps_resp.text))

        for lap_data in laps_data["lapDTOs"]:
            lap = Lap()
            if "startTimeGMT" in lap_data:
                lap.StartTime = pytz.utc.localize(datetime.strptime(lap_data["startTimeGMT"], "%Y-%m-%dT%H:%M:%S.0"))

            elapsed_duration = None
            if "elapsedDuration" in lap_data:
                elapsed_duration = timedelta(seconds=round(float(lap_data["elapsedDuration"])))
            elif "duration" in lap_data:
                elapsed_duration = timedelta(seconds=round(float(lap_data["duration"])))

            if lap.StartTime and elapsed_duration:
                # Always recalculate end time based on duration, if we have the start time
                lap.EndTime = lap.StartTime + elapsed_duration
            if not lap.StartTime and lap.EndTime and elapsed_duration:
                # Sometimes calculate start time based on duration
                lap.StartTime = lap.EndTime - elapsed_duration

            if not lap.StartTime or not lap.EndTime:
                # Garmin Connect is weird.
                raise APIExcludeActivity("Activity lap has no BeginTimestamp or EndTimestamp", user_exception=UserException(UserExceptionType.Corrupt))

            applyStats(lap_data, lap.Stats)
            activity.Laps.append(lap)

        # In Garmin Land, max can be smaller than min for this field :S
        if activity.Stats.Power.Max is not None and activity.Stats.Power.Min is not None and activity.Stats.Power.Min > activity.Stats.Power.Max:
            activity.Stats.Power.Min = None

    def DownloadActivity(self, serviceRecord, activity):
        # First, download the summary stats and lap stats
        self._downloadActivitySummary(serviceRecord, activity)

        if len(activity.Laps) == 1:
            activity.Stats = activity.Laps[0].Stats # They must be identical to pass the verification

        if activity.Stationary:
            # Nothing else to download
            return activity

        # https://connect.garmin.com/proxy/activity-service-1.3/json/activityDetails/####
        activityID = activity.ServiceData["ActivityID"]
        res = self._request_with_reauth(serviceRecord, lambda session: session.get("https://connect.garmin.com/modern/proxy/activity-service-1.3/json/activityDetails/" + str(activityID) + "?maxSize=999999999"))
        try:
            raw_data = res.json()["com.garmin.activity.details.json.ActivityDetails"]
        except ValueError:
            raise APIException("Activity data parse error for %s: %s" % (res.status_code, res.text))

        if "measurements" not in raw_data:
            activity.Stationary = True # We were wrong, oh well
            return activity

        attrs_map = {}
        def _map_attr(gc_key, wp_key, units, in_location=False, is_timestamp=False):
            attrs_map[gc_key] = {
                "key": wp_key,
                "to_units": units,
                "in_location": in_location, # Blegh
                "is_timestamp": is_timestamp # See above
            }

        _map_attr("directSpeed", "Speed", ActivityStatisticUnit.MetersPerSecond)
        _map_attr("sumDistance", "Distance", ActivityStatisticUnit.Meters)
        _map_attr("directHeartRate", "HR", ActivityStatisticUnit.BeatsPerMinute)
        _map_attr("directBikeCadence", "Cadence", ActivityStatisticUnit.RevolutionsPerMinute)
        _map_attr("directDoubleCadence", "RunCadence", ActivityStatisticUnit.StepsPerMinute) # 2*x mystery solved
        _map_attr("directAirTemperature", "Temp", ActivityStatisticUnit.DegreesCelcius)
        _map_attr("directPower", "Power", ActivityStatisticUnit.Watts)
        _map_attr("directElevation", "Altitude", ActivityStatisticUnit.Meters, in_location=True)
        _map_attr("directLatitude", "Latitude", None, in_location=True)
        _map_attr("directLongitude", "Longitude", None, in_location=True)
        _map_attr("directTimestamp", "Timestamp", None, is_timestamp=True)

        # Figure out which metrics we'll be seeing in this activity
        attrs_indexed = {}
        for measurement in raw_data["measurements"]:
            key = measurement["key"]
            if key in attrs_map:
                if attrs_map[key]["to_units"]:
                    attrs_map[key]["from_units"] = self._unitMap[measurement["unit"]]
                    if attrs_map[key]["to_units"] == attrs_map[key]["from_units"]:
                        attrs_map[key]["to_units"] = attrs_map[key]["from_units"] = None
                attrs_indexed[measurement["metricsIndex"]] = attrs_map[key]

        # Process the data frames
        frame_idx = 0
        active_lap_idx = 0
        for frame in raw_data["metrics"]:
            wp = Waypoint()
            for idx, attr in attrs_indexed.items():
                value = frame["metrics"][idx]
                target_obj = wp
                if attr["in_location"]:
                    if not wp.Location:
                        wp.Location = Location()
                    target_obj = wp.Location

                # Handle units
                if attr["is_timestamp"]:
                    value = pytz.utc.localize(datetime.utcfromtimestamp(value / 1000))
                elif attr["to_units"]:
                    value = ActivityStatistic.convertValue(value, attr["from_units"], attr["to_units"])

                # Write the value (can't use __dict__ because __slots__)
                setattr(target_obj, attr["key"], value)

            # Fix up lat/lng being zero (which appear to represent missing coords)
            if wp.Location and wp.Location.Latitude == 0 and wp.Location.Longitude == 0:
                wp.Location.Latitude = None
                wp.Location.Longitude = None
            # Please visit a physician before complaining about this
            if wp.HR == 0:
                wp.HR = None
            # Bump the active lap if required
            while (active_lap_idx < len(activity.Laps) - 1 and # Not the last lap
                   activity.Laps[active_lap_idx + 1].StartTime <= wp.Timestamp):
                active_lap_idx += 1
            activity.Laps[active_lap_idx].Waypoints.append(wp)
            frame_idx += 1

        return activity

    def UploadActivity(self, serviceRecord, activity):
        #/proxy/upload-service-1.1/json/upload/.fit
        fit_file = FITIO.Dump(activity)
        files = {"data": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", fit_file)}

        res = self._request_with_reauth(serviceRecord, lambda session: session.post("https://connect.garmin.com/proxy/upload-service-1.1/json/upload/.fit", files=files))
        res = res.json()["detailedImportResult"]

        if len(res["successes"]) == 0:
            if len(res["failures"]) and len(res["failures"][0]["messages"]) and res["failures"][0]["messages"][0]["content"] == "Duplicate activity":
                logger.debug("Duplicate")
                return # ...cool?
            raise APIException("Unable to upload activity %s" % res)
        if len(res["successes"]) > 1:
            raise APIException("Uploaded succeeded, resulting in too many activities")
        actid = res["successes"][0]["internalId"]

        name = activity.Name # Capture in logs
        notes = activity.Notes

        # Update activity metadata not included in the FIT file.
        metadata_object = {}
        if activity.Name and activity.Name.strip():
            metadata_object["activityName"] = activity.Name
        if activity.Notes and activity.Notes.strip():
            metadata_object["description"] = activity.Notes
        if activity.Type not in [ActivityType.Running, ActivityType.Cycling, ActivityType.Other]:
            # Set the legit activity type - whatever it is, it's not supported by the FIT schema
            acttype = [k for k, v in self._reverseActivityMappings.items() if v == activity.Type]
            if len(acttype) == 0:
                raise APIWarning("GarminConnect does not support activity type " + activity.Type)
            else:
                acttype = acttype[0]
            metadata_object["activityTypeDTO"] = {"typeKey": acttype}
        if activity.Private:
            metadata_object["accessControlRuleDTO"] = {"typeKey": "private"}

        if metadata_object:
            metadata_object["activityId"] = actid
            encoding_headers = {"Content-Type": "application/json; charset=UTF-8"} # GC really, really needs this part, otherwise it throws obscure errors like "Invalid signature for signature method HMAC-SHA1"
            res = self._request_with_reauth(serviceRecord, lambda session: session.put("https://connect.garmin.com/proxy/activity-service/activity/" + str(actid), data=json.dumps(metadata_object), headers=encoding_headers))
            if res.status_code != 204:
                raise APIWarning("Unable to set activity metadata - %d %s" % (res.status_code, res.text))

        return actid

    def _user_watch_user(self, serviceRecord):
        if not serviceRecord.GetConfiguration()["WatchUserKey"]:
            user_key = random.choice(list(GARMIN_CONNECT_USER_WATCH_ACCOUNTS.keys()))
            logger.info("Assigning %s a new watch user %s" % (serviceRecord.ExternalID, user_key))
            serviceRecord.SetConfiguration({"WatchUserKey": user_key})
            return GARMIN_CONNECT_USER_WATCH_ACCOUNTS[user_key]
        else:
            return GARMIN_CONNECT_USER_WATCH_ACCOUNTS[serviceRecord.GetConfiguration()["WatchUserKey"]]

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        # PUT http://connect.garmin.com/proxy/userprofile-service/connection/request/cpfair
        # (the poll worker finishes the connection)
        user_name = self._user_watch_user(serviceRecord)["Name"]
        logger.info("Requesting connection to %s from %s" % (user_name, serviceRecord.ExternalID))
        self._rate_limit()
        resp = self._get_session(record=serviceRecord, skip_cache=True).put("https://connect.garmin.com/proxy/userprofile-service/connection/request/%s" % user_name)
        try:
            assert resp.status_code == 200
            assert resp.json()["requestStatus"] == "Created"
        except:
            raise APIException("Connection request failed with user watch account %s: %s %s" % (user_name, resp.status_code, resp.text))
        else:
            serviceRecord.SetConfiguration({"WatchConnectionID": resp.json()["id"]})

        serviceRecord.SetPartialSyncTriggerSubscriptionState(True)

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        # GET http://connect.garmin.com/proxy/userprofile-service/socialProfile/connections to get the ID
        #  {"fullName":null,"userConnections":[{"userId":5754439,"displayName":"TapiirikAPITEST","fullName":null,"location":null,"profileImageUrlMedium":null,"profileImageUrlSmall":null,"connectionRequestId":1566024,"userConnectionStatus":2,"userRoles":["ROLE_CONNECTUSER","ROLE_FITNESS_USER"],"userPro":false}]}
        # PUT http://connect.garmin.com/proxy/userprofile-service/connection/end/1904201
        # Unfortunately there's no way to delete a pending request - the poll worker will do this from the other end
        active_watch_user = self._user_watch_user(serviceRecord)
        session = self._get_session(email=active_watch_user["Username"], password=active_watch_user["Password"], skip_cache=True)
        if "WatchConnectionID" in serviceRecord.GetConfiguration():
            self._rate_limit()
            dc_resp = session.put("https://connect.garmin.com/modern/proxy/userprofile-service/connection/end/%s" % serviceRecord.GetConfiguration()["WatchConnectionID"])
            if dc_resp.status_code != 200:
                raise APIException("Error disconnecting user watch accunt %s from %s: %s %s" % (active_watch_user, serviceRecord.ExternalID, dc_resp.status_code, dc_resp.text))

            serviceRecord.SetConfiguration({"WatchUserKey": None, "WatchConnectionID": None})
            serviceRecord.SetPartialSyncTriggerSubscriptionState(False)
        else:
            # I broke Garmin Connect by having too many connections per account, so I can no longer query the connection list
            # All the connection request emails are sitting unopened in an email inbox, though, so I'll be backfilling the IDs from those
            raise APIException("Did not store connection ID")

    def ShouldForcePartialSyncTrigger(self, serviceRecord):
        # The poll worker can't see private activities.
        return serviceRecord.GetConfiguration()["sync_private"]


    def PollPartialSyncTrigger(self, multiple_index):
        # TODO: ensure the appropriate users are connected
        # GET http://connect.garmin.com/modern/proxy/userprofile-service/connection/pending to get ID
        #  [{"userId":6244126,"displayName":"tapiriik-sync-ulukhaktok","fullName":"tapiriik sync ulukhaktok","profileImageUrlSmall":null,"connectionRequestId":1904086,"requestViewed":true,"userRoles":["ROLE_CONNECTUSER"],"userPro":false}]
        # PUT http://connect.garmin.com/proxy/userprofile-service/connection/accept/1904086
        # ...later...
        # GET http://connect.garmin.com/proxy/activitylist-service/activities/comments/subscriptionFeed?start=1&limit=10

        # First, accept any pending connections
        watch_user_key = sorted(list(GARMIN_CONNECT_USER_WATCH_ACCOUNTS.keys()))[multiple_index]
        watch_user = GARMIN_CONNECT_USER_WATCH_ACCOUNTS[watch_user_key]
        session = self._get_session(email=watch_user["Username"], password=watch_user["Password"], skip_cache=True)

        # These seems to fail with a 500 (talkking about a timeout) the first time, so keep trying.
        SERVER_ERROR_RETRIES = 10
        # Then, check for users with new activities
        for x in range(SERVER_ERROR_RETRIES):
            self._rate_limit()
            watch_activities_resp = session.get("https://connect.garmin.com/modern/proxy/activitylist-service/activities/subscriptionFeed?limit=1000")
            if watch_activities_resp.status_code != 500:
                break
        try:
            watch_activities = watch_activities_resp.json()
        except ValueError:
            raise Exception("Could not parse new activities list: %s %s" % (watch_activities_resp.status_code, watch_activities_resp.text))

        active_user_pairs = [(x["ownerDisplayName"], x["activityId"]) for x in watch_activities["activityList"]]
        active_user_pairs.sort(key=lambda x: x[1]) # Highest IDs last (so they make it into the dict, supplanting lower IDs where appropriate)
        active_users = dict(active_user_pairs)

        active_user_recs = [ServiceRecord(x) for x in db.connections.find({"ExternalID": {"$in": list(active_users.keys())}, "Service": "garminconnect"}, {"Config": 1, "ExternalID": 1, "Service": 1})]

        if len(active_user_recs) != len(active_users.keys()):
            logger.warning("Mismatch %d records found for %d active users" % (len(active_user_recs), len(active_users.keys())))

        to_sync_ids = []
        for active_user_rec in active_user_recs:
            last_active_id = active_user_rec.GetConfiguration()["WatchUserLastID"]
            this_active_id = active_users[active_user_rec.ExternalID]
            if this_active_id > last_active_id:
                to_sync_ids.append(active_user_rec.ExternalID)
                active_user_rec.SetConfiguration({"WatchUserLastID": this_active_id, "WatchUserKey": watch_user_key})

        self._rate_limit()
        for x in range(SERVER_ERROR_RETRIES):
            pending_connections_resp = session.get("https://connect.garmin.com/modern/proxy/userprofile-service/connection/pending")
            if pending_connections_resp.status_code != 500:
                break
        try:
            pending_connections = pending_connections_resp.json()
        except ValueError:
            logger.error("Could not parse pending connection requests: %s %s" % (pending_connections_resp.status_code, pending_connections_resp.text))
        else:
            valid_pending_connections_external_ids = [x["ExternalID"] for x in db.connections.find({"Service": "garminconnect", "ExternalID": {"$in": [x["displayName"] for x in pending_connections]}}, {"ExternalID": 1})]
            logger.info("Accepting %d, denying %d connection requests for %s" % (len(valid_pending_connections_external_ids), len(pending_connections) - len(valid_pending_connections_external_ids), watch_user_key))
            for pending_connect in pending_connections:
                if pending_connect["displayName"] in valid_pending_connections_external_ids:
                    self._rate_limit()
                    connect_resp = session.put("https://connect.garmin.com/modern/proxy/userprofile-service/connection/accept/%s" % pending_connect["connectionRequestId"])
                    if connect_resp.status_code != 200:
                        logger.error("Error accepting request on watch account %s: %s %s" % (watch_user["Name"], connect_resp.status_code, connect_resp.text))
                else:
                    self._rate_limit()
                    session.put("https://connect.garmin.com/modern/proxy/userprofile-service/connection/decline/%s" % pending_connect["connectionRequestId"])


        return to_sync_ids

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        session = self._get_session(record=serviceRecord)
        self._rate_limit()
        del_res = session.delete("https://connect.garmin.com/modern/proxy/activity-service/activity/%d" % uploadId)
        del_res.raise_for_status()
