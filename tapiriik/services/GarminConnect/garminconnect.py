from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, APIWarning, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.statistic_calculator import ActivityStatisticCalculator
from tapiriik.services.tcx import TCXIO
from tapiriik.services.gpx import GPXIO
from tapiriik.services.fit import FITIO
from tapiriik.services.sessioncache import SessionCache
from tapiriik.database import cachedb

from django.core.urlresolvers import reverse
import pytz
from datetime import datetime, timedelta
import requests
import os
import math
import logging
import time
import json
logger = logging.getLogger(__name__)

class GarminConnectService(ServiceBase):
    ID = "garminconnect"
    DisplayName = "Garmin Connect"
    DisplayAbbreviation = "GC"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

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
                                "all": ActivityType.Other  # everything will eventually resolve to this
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
                                "other": ActivityType.Other  # I guess? (vs. "all" that is)
    }

    SupportedActivities = list(_activityMappings.values())

    SupportsHR = SupportsCadence = True

    _sessionCache = SessionCache(lifetime=timedelta(minutes=30), freshen_on_get=True)

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
        "stepsPerMinute": ActivityStatisticUnit.StepsPerMinute,
        "rpm": ActivityStatisticUnit.RevolutionsPerMinute,
        "watt": ActivityStatisticUnit.Watts
    }

    def __init__(self):
        cachedHierarchy = cachedb.gc_type_hierarchy.find_one()
        if not cachedHierarchy:
            rawHierarchy = requests.get("http://connect.garmin.com/proxy/activity-service-1.2/json/activity_types").text
            self._activityHierarchy = json.loads(rawHierarchy)["dictionary"]
            cachedb.gc_type_hierarchy.insert({"Hierarchy": rawHierarchy})
        else:
            self._activityHierarchy = json.loads(cachedHierarchy["Hierarchy"])["dictionary"]
        self._rate_lock = open("/tmp/gc_rate.lock", "w")

    def _rate_limit(self):
        import fcntl
        print("Waiting for lock")
        fcntl.flock(self._rate_lock,fcntl.LOCK_EX)
        try:
            print("Have lock")
            time.sleep(1) # I appear to been banned from Garmin Connect while determining this.
            print("Rate limited")
        finally:
            fcntl.flock(self._rate_lock,fcntl.LOCK_UN)

    def _get_cookies(self, record=None, email=None, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
        params = {"login": "login", "login:loginUsernameField": email, "login:password": password, "login:signInButton": "Sign In", "javax.faces.ViewState": "j_id1"}
        self._rate_limit()
        preResp = requests.get("https://connect.garmin.com/signin")
        auth_retries = 3 # Did I mention Garmin Connect is silly?
        for retries in range(auth_retries):
            self._rate_limit()
            resp = requests.post("https://connect.garmin.com/signin", data=params, allow_redirects=False, cookies=preResp.cookies)
            if resp.status_code >= 500 and resp.status_code < 600:
                raise APIException("Remote API failure")
            if resp.status_code != 302:  # yep
                if "errorMessage" in resp.text:
                    if retries < auth_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                else:
                    raise APIException("Mystery login error %s" % resp.text)
            break
        if record:
            self._sessionCache.Set(record.ExternalID, preResp.cookies)
        return preResp.cookies

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        cookies = self._get_cookies(email=email, password=password)
        self._rate_limit()
        username = requests.get("http://connect.garmin.com/user/username", cookies=cookies).json()["username"]
        if not len(username):
            raise APIException("Unable to retrieve username", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (username, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})


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
        cookies = self._get_cookies(record=serviceRecord)
        page = 1
        pageSz = 100
        activities = []
        exclusions = []
        while True:
            logger.debug("Req with " + str({"start": (page - 1) * pageSz, "limit": pageSz}))
            self._rate_limit()
            res = requests.get("http://connect.garmin.com/proxy/activity-search-service-1.0/json/activities", params={"start": (page - 1) * pageSz, "limit": pageSz}, cookies=cookies)
            res = res.json()["results"]
            if "activities" not in res:
                break  # No activities on this page - empty account.
            for act in res["activities"]:
                act = act["activity"]
                if "sumDistance" not in act:
                    exclusions.append(APIExcludeActivity("No distance", activityId=act["activityId"], userException=UserException(UserExceptionType.Corrupt)))
                    continue
                activity = UploadedActivity()

                if "sumSampleCountSpeed" not in act and "sumSampleCountTimestamp" not in act: # Don't really know why sumSampleCountTimestamp doesn't appear in swim activities - they're definitely timestamped...
                    activity.Stationary = True
                else:
                    activity.Stationary = False

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
                # TODO: fix the distance stats to account for the fact that this incorrectly reported km instead of meters for the longest time.
                activity.Stats.Distance = ActivityStatistic(self._unitMap[act["sumDistance"]["uom"]], value=float(act["sumDistance"]["value"]))

                def mapStat(gcKey, statKey, type, useSourceUnits=False):
                    nonlocal activity, act
                    if gcKey in act:
                        value = float(act[gcKey]["value"])
                        if math.isinf(value):
                            return # GC returns the minimum speed as "-Infinity" instead of 0 some times :S
                        activity.Stats.__dict__[statKey].update(ActivityStatistic(self._unitMap[act[gcKey]["uom"]], **({type: value})))
                        if useSourceUnits:
                            activity.Stats.__dict__[statKey] = activity.Stats.__dict__[statKey].asUnits(self._unitMap[act[gcKey]["uom"]])

                if "sumMovingDuration" in act:
                    activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=float(act["sumMovingDuration"]["value"])))

                if "sumDuration" in act:
                    activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(minutes=float(act["sumDuration"]["minutesSeconds"].split(":")[0]), seconds=float(act["sumDuration"]["minutesSeconds"].split(":")[1])))


                mapStat("minSpeed", "Speed", "min", useSourceUnits=True) # We need to suppress conversion here, so we can fix the pace-speed issue below
                mapStat("maxSpeed", "Speed", "max", useSourceUnits=True)
                mapStat("weightedMeanSpeed", "Speed", "avg", useSourceUnits=True)
                mapStat("minAirTemperature", "Temperature", "min")
                mapStat("maxAirTemperature", "Temperature", "max")
                mapStat("weightedMeanAirTemperature", "Temperature", "avg")
                mapStat("sumEnergy", "Energy", "value")
                mapStat("maxHeartRate", "HR", "max")
                mapStat("weightedMeanHeartRate", "HR", "avg")
                mapStat("maxRunCadence", "RunCadence", "max")
                mapStat("weightedMeanRunCadence", "RunCadence", "avg")
                mapStat("maxBikeCadence", "Cadence", "max")
                mapStat("weightedMeanBikeCadence", "Cadence", "avg")
                mapStat("minPower", "Power", "min")
                mapStat("maxPower", "Power", "max")
                mapStat("weightedMeanPower", "Power", "avg")
                mapStat("minElevation", "Elevation", "min")
                mapStat("maxElevation", "Elevation", "max")
                mapStat("gainElevation", "Elevation", "gain")
                mapStat("lossElevation", "Elevation", "loss")

                # In Garmin Land, max can be smaller than min for this field :S
                if activity.Stats.Power.Max is not None and activity.Stats.Power.Min is not None and activity.Stats.Power.Min > activity.Stats.Power.Max:
                    activity.Stats.Power.Min = None

                # To get it to match what the user sees in GC.
                if activity.Stats.RunCadence.Max is not None:
                    activity.Stats.RunCadence.Max *= 2
                if activity.Stats.RunCadence.Average is not None:
                    activity.Stats.RunCadence.Average *= 2

                # GC incorrectly reports pace measurements as kph/mph when they are in fact in min/km or min/mi
                if "minSpeed" in act:
                    if ":" in act["minSpeed"]["withUnitAbbr"] and activity.Stats.Speed.Min:
                        activity.Stats.Speed.Min = 60 / activity.Stats.Speed.Min
                if "maxSpeed" in act:
                    if ":" in act["maxSpeed"]["withUnitAbbr"] and activity.Stats.Speed.Max:
                        activity.Stats.Speed.Max = 60 / activity.Stats.Speed.Max
                if "weightedMeanSpeed" in act:
                    if ":" in act["weightedMeanSpeed"]["withUnitAbbr"] and activity.Stats.Speed.Average:
                        activity.Stats.Speed.Average = 60 / activity.Stats.Speed.Average

                # Similarly, they do weird stuff with HR at times - %-of-max and zones
                # ...and we can't just fix these, so we have to calculate it after the fact (blegh)
                recalcHR = False
                if "maxHeartRate" in act:
                    if "%" in act["maxHeartRate"]["withUnitAbbr"] or "z" in act["maxHeartRate"]["withUnitAbbr"]:
                        activity.Stats.HR.Max = None
                        recalcHR = True
                if "weightedMeanHeartRate" in act:
                    if "%" in act["weightedMeanHeartRate"]["withUnitAbbr"] or "z" in act["weightedMeanHeartRate"]["withUnitAbbr"]:
                        activity.Stats.HR.Average = None
                        recalcHR = True


                activity.Type = self._resolveActivityType(act["activityType"]["key"])

                activity.CalculateUID()
                activity.ServiceData = {"ActivityID": act["activityId"], "RecalcHR": recalcHR}

                activities.append(activity)
            logger.debug("Finished page " + str(page) + " of " + str(res["search"]["totalPages"]))
            if not exhaustive or int(res["search"]["totalPages"]) == page:
                break
            else:
                page += 1
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        #http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/#####?full=true
        activityID = activity.ServiceData["ActivityID"]
        cookies = self._get_cookies(record=serviceRecord)
        self._rate_limit()
        res = requests.get("http://connect.garmin.com/proxy/activity-service-1.1/tcx/activity/" + str(activityID) + "?full=true", cookies=cookies)
        try:
            TCXIO.Parse(res.content, activity)
        except ValueError as e:
            raise APIExcludeActivity("TCX parse error " + str(e), userException=UserException(UserExceptionType.Corrupt))

        if activity.ServiceData["RecalcHR"]:
            logger.debug("Recalculating HR")
            avgHR, maxHR = ActivityStatisticCalculator.CalculateAverageMaxHR(activity)
            activity.Stats.HR.coalesceWith(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=maxHR, avg=avgHR))

        if len(activity.Laps) == 1:
            activity.Laps[0].Stats.update(activity.Stats) # I trust Garmin Connect's stats more than whatever shows up in the TCX
            activity.Stats = activity.Laps[0].Stats # They must be identical to pass the verification

        if activity.Stats.Temperature.Min is not None or activity.Stats.Temperature.Max is not None or activity.Stats.Temperature.Average is not None:
            logger.debug("Retrieving additional temperature data")
            # TCX doesn't have temperature, for whatever reason...
            self._rate_limit()
            res = requests.get("http://connect.garmin.com/proxy/activity-service-1.1/gpx/activity/" + str(activityID) + "?full=true", cookies=cookies)
            try:
                temp_act = GPXIO.Parse(res.content, suppress_validity_errors=True)
            except ValueError as e:
                raise APIExcludeActivity("GPX parse error " + str(e), userException=UserException(UserExceptionType.Corrupt))

            logger.debug("Merging additional temperature data")
            full_waypoints = activity.GetFlatWaypoints()
            temp_waypoints = temp_act.GetFlatWaypoints()

            merge_idx = 0

            for x in range(len(temp_waypoints)):
                while full_waypoints[merge_idx].Timestamp < temp_waypoints[x].Timestamp and merge_idx < len(full_waypoints) - 1:
                    merge_idx += 1
                full_waypoints[merge_idx].Temp = temp_waypoints[x].Temp



        return activity

    def UploadActivity(self, serviceRecord, activity):
        #/proxy/upload-service-1.1/json/upload/.fit
        fit_file = FITIO.Dump(activity)
        files = {"data": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", fit_file)}
        cookies = self._get_cookies(record=serviceRecord)
        self._rate_limit()
        res = requests.post("http://connect.garmin.com/proxy/upload-service-1.1/json/upload/.tcx", files=files, cookies=cookies)
        res = res.json()["detailedImportResult"]

        if len(res["successes"]) == 0:
            raise APIException("Unable to upload activity")
        if len(res["successes"]) > 1:
            raise APIException("Uploaded succeeded, resulting in too many activities")
        actid = res["successes"][0]["internalId"]

        encoding_headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"} # GC really, really needs this part, otherwise it throws obscure errors like "Invalid signature for signature method HMAC-SHA1"
        warnings = []
        try:
            if activity.Name and activity.Name.strip():
                self._rate_limit()
                res = requests.post("http://connect.garmin.com/proxy/activity-service-1.2/json/name/" + str(actid), data={"value": activity.Name.encode("UTF-8")}, cookies=cookies, headers=encoding_headers)
                try:
                    res = res.json()
                except:
                    raise APIWarning("Activity name request failed - %s" % res.text)
                if "display" not in res or res["display"]["value"] != activity.Name:
                    raise APIWarning("Unable to set activity name")
        except APIWarning as e:
            warnings.append(e)

        try:
            if activity.Notes and activity.Notes.strip():
                self._rate_limit()
                res = requests.post("https://connect.garmin.com/proxy/activity-service-1.2/json/description/" + str(actid), data={"value": activity.Notes.encode("UTF-8")}, cookies=cookies, headers=encoding_headers)
                try:
                    res = res.json()
                except:
                    raise APIWarning("Activity notes request failed - %s" % res.text)
                if "display" not in res or res["display"]["value"] != activity.Notes:
                    raise APIWarning("Unable to set activity notes")
        except APIWarning as e:
            warnings.append(e)

        try:
            if activity.Type not in [ActivityType.Running, ActivityType.Cycling, ActivityType.Other]:
                # Set the legit activity type - whatever it is, it's not supported by the TCX schema
                acttype = [k for k, v in self._reverseActivityMappings.items() if v == activity.Type]
                if len(acttype) == 0:
                    raise APIWarning("GarminConnect does not support activity type " + activity.Type)
                else:
                    acttype = acttype[0]
                self._rate_limit()
                res = requests.post("https://connect.garmin.com/proxy/activity-service-1.2/json/type/" + str(actid), data={"value": acttype}, cookies=cookies)
                res = res.json()
                if "activityType" not in res or res["activityType"]["key"] != acttype:
                    raise APIWarning("Unable to set activity type")
        except APIWarning as e:
            warnings.append(e)

        if len(warnings):
            raise APIWarning(str(warnings)) # Meh
        return actid



    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def DeleteCachedData(self, serviceRecord):
        # nothing cached...
        pass
