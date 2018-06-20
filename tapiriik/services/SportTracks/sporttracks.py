from tapiriik.settings import WEB_ROOT, SPORTTRACKS_OPENFIT_ENDPOINT, SPORTTRACKS_CLIENT_ID, SPORTTRACKS_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, LapIntensity, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.sessioncache import SessionCache
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
import pytz
from datetime import timedelta
import dateutil.parser
from dateutil.tz import tzutc
import requests
import json
import re
import urllib.parse

import logging
logger = logging.getLogger(__name__)

class SportTracksService(ServiceBase):
    ID = "sporttracks"
    DisplayName = "SportTracks"
    DisplayAbbreviation = "ST"
    AuthenticationType = ServiceAuthenticationType.OAuth
    OpenFitEndpoint = SPORTTRACKS_OPENFIT_ENDPOINT
    SupportsHR = True
    AuthenticationNoFrame = True

    """ Other   Basketball
        Other   Boxing
        Other   Climbing
        Other   Driving
        Other   Flying
        Other   Football
        Other   Gardening
        Other   Kitesurf
        Other   Sailing
        Other   Soccer
        Other   Tennis
        Other   Volleyball
        Other   Windsurf
        Running Hashing
        Running Hills
        Running Intervals
        Running Orienteering
        Running Race
        Running Road
        Running Showshoe
        Running Speed
        Running Stair
        Running Track
        Running Trail
        Running Treadmill
        Cycling Hills
        Cycling Indoor
        Cycling Intervals
        Cycling Mountain
        Cycling Race
        Cycling Road
        Cycling Rollers
        Cycling Spinning
        Cycling Track
        Cycling Trainer
        Swimming    Open Water
        Swimming    Pool
        Swimming    Race
        Walking Geocaching
        Walking Hiking
        Walking Nordic
        Walking Photography
        Walking Snowshoe
        Walking Treadmill
        Skiing  Alpine
        Skiing  Nordic
        Skiing  Roller
        Skiing  Snowboard
        Rowing  Canoe
        Rowing  Kayak
        Rowing  Kitesurf
        Rowing  Ocean Kayak
        Rowing  Rafting
        Rowing  Rowing Machine
        Rowing  Sailing
        Rowing  Standup Paddling
        Rowing  Windsurf
        Skating Board
        Skating Ice
        Skating Inline
        Skating Race
        Skating Track
        Gym Aerobics
        Gym Elliptical
        Gym Plyometrics
        Gym Rowing Machine
        Gym Spinning
        Gym Stair Climber
        Gym Stationary Bike
        Gym Strength
        Gym Stretching
        Gym Treadmill
        Gym Yoga
    """

    _activityMappings = {
        "running": ActivityType.Running,
        "cycling": ActivityType.Cycling,
        "mountain": ActivityType.MountainBiking,
        "walking": ActivityType.Walking,
        "hiking": ActivityType.Hiking,
        "snowboarding": ActivityType.Snowboarding,
        "skiing": ActivityType.DownhillSkiing,
        "nordic": ActivityType.CrossCountrySkiing,
        "skating": ActivityType.Skating,
        "swimming": ActivityType.Swimming,
        "rowing": ActivityType.Rowing,
        "elliptical": ActivityType.Elliptical,
        "gym": ActivityType.Gym,
        "standup paddling": ActivityType.StandUpPaddling,
        "other": ActivityType.Other
    }

    _reverseActivityMappings = {
        ActivityType.Running: "running",
        ActivityType.Cycling: "cycling",
        ActivityType.Walking: "walking",
        ActivityType.MountainBiking: "cycling: mountain",
        ActivityType.Hiking: "walking: hiking",
        ActivityType.CrossCountrySkiing: "skiing: nordic",  #  Equipment.Bindings.IsToeOnly ??
        ActivityType.DownhillSkiing: "skiing",
        ActivityType.Snowboarding: "skiing: snowboarding",
        ActivityType.Skating: "skating",
        ActivityType.Swimming: "swimming",
        ActivityType.Rowing: "rowing",
        ActivityType.Elliptical: "gym: elliptical",
        ActivityType.Gym: "gym",
        ActivityType.StandUpPaddling: "rowing: standup paddling",
        ActivityType.Other: "other"
    }

    SupportedActivities = list(_reverseActivityMappings.keys())

    _tokenCache = SessionCache("sporttracks", lifetime=timedelta(minutes=115), freshen_on_get=False)

    def WebInit(self):
        self.UserAuthorizationURL = "https://api.sporttracks.mobi/oauth2/authorize?response_type=code&client_id=%s&state=mobi_api" % SPORTTRACKS_CLIENT_ID

    def _getAuthHeaders(self, serviceRecord=None):
        token = self._tokenCache.Get(serviceRecord.ExternalID)
        if not token:
            if not serviceRecord.Authorization or "RefreshToken" not in serviceRecord.Authorization:
                # When I convert the existing users, people who didn't check the remember-credentials box will be stuck in limbo
                raise APIException("User not upgraded to OAuth", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            # Use refresh token to get access token
            # Hardcoded return URI to get around the lack of URL reversing without loading up all the Django stuff
            params = {"grant_type": "refresh_token", "refresh_token": serviceRecord.Authorization["RefreshToken"], "client_id": SPORTTRACKS_CLIENT_ID, "client_secret": SPORTTRACKS_CLIENT_SECRET, "redirect_uri": "https://tapiriik.com/auth/return/sporttracks"}
            response = requests.post("https://api.sporttracks.mobi/oauth2/token", data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
            if response.status_code != 200:
                if response.status_code >= 400 and response.status_code < 500:
                    raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text))
            token = response.json()["access_token"]
            self._tokenCache.Set(serviceRecord.ExternalID, token)

        return {"Authorization": "Bearer %s" % token}

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service
        #  might consider a real OAuth client
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": SPORTTRACKS_CLIENT_ID, "client_secret": SPORTTRACKS_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "sporttracks"})}

        response = requests.post("https://api.sporttracks.mobi/oauth2/token", data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if response.status_code != 200:
            print(response.text)
            raise APIException("Invalid code")
        access_token = response.json()["access_token"]
        refresh_token = response.json()["refresh_token"]

        uid_res = requests.post("https://api.sporttracks.mobi/api/v2/system/connect", headers={"Authorization": "Bearer %s" % access_token})
        uid = uid_res.json()["user"]["uid"]

        return (uid, {"RefreshToken": refresh_token})

    def RevokeAuthorization(self, serviceRecord):
        pass  # Can't revoke these tokens :(

    def DeleteCachedData(self, serviceRecord):
        cachedb.sporttracks_meta_cache.remove({"ExternalID": serviceRecord.ExternalID})

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        headers = self._getAuthHeaders(serviceRecord)
        activities = []
        exclusions = []
        pageUri = self.OpenFitEndpoint + "/fitnessActivities.json"

        activity_tz_cache_raw = cachedb.sporttracks_meta_cache.find_one({"ExternalID": serviceRecord.ExternalID})
        activity_tz_cache_raw = activity_tz_cache_raw if activity_tz_cache_raw else {"Activities":[]}
        activity_tz_cache = dict([(x["ActivityURI"], x["TZ"]) for x in activity_tz_cache_raw["Activities"]])

        while True:
            logger.debug("Req against " + pageUri)
            res = requests.get(pageUri, headers=headers)
            try:
                res = res.json()
            except ValueError:
                raise APIException("Could not decode activity list response %s %s" % (res.status_code, res.text))
            for act in res["items"]:
                activity = UploadedActivity()
                activity.ServiceData = {"ActivityURI": act["uri"]}

                if len(act["name"].strip()):
                    activity.Name = act["name"]
                    # Longstanding ST.mobi bug causes it to return negative partial-hour timezones as "-2:-30" instead of "-2:30"
                fixed_start_time = re.sub(r":-(\d\d)", r":\1", act["start_time"])
                activity.StartTime = dateutil.parser.parse(fixed_start_time)
                if isinstance(activity.StartTime.tzinfo, tzutc):
                    activity.TZ = pytz.utc # The dateutil tzutc doesn't have an _offset value.
                else:
                    activity.TZ = pytz.FixedOffset(activity.StartTime.tzinfo.utcoffset(activity.StartTime).total_seconds() / 60)  # Convert the dateutil lame timezones into pytz awesome timezones.

                activity.StartTime = activity.StartTime.replace(tzinfo=activity.TZ)
                activity.EndTime = activity.StartTime + timedelta(seconds=float(act["duration"]))
                activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=float(act["duration"]))  # OpenFit says this excludes paused times.

                # Sometimes activities get returned with a UTC timezone even when they are clearly not in UTC.
                if activity.TZ == pytz.utc:
                    if act["uri"] in activity_tz_cache:
                        activity.TZ = pytz.FixedOffset(activity_tz_cache[act["uri"]])
                    else:
                        # So, we get the first location in the activity and calculate the TZ from that.
                        try:
                            firstLocation = self._downloadActivity(serviceRecord, activity, returnFirstLocation=True)
                        except APIExcludeActivity:
                            pass
                        else:
                            try:
                                activity.CalculateTZ(firstLocation, recalculate=True)
                            except:
                                # We tried!
                                pass
                            else:
                                activity.AdjustTZ()
                            finally:
                                activity_tz_cache[act["uri"]] = activity.StartTime.utcoffset().total_seconds() / 60

                logger.debug("Activity s/t " + str(activity.StartTime))
                activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=float(act["total_distance"]))

                types = [x.strip().lower() for x in act["type"].split(":")]
                types.reverse()  # The incoming format is like "walking: hiking" and we want the most specific first
                activity.Type = None
                for type_key in types:
                    if type_key in self._activityMappings:
                        activity.Type = self._activityMappings[type_key]
                        break
                if not activity.Type:
                    exclusions.append(APIExcludeActivity("Unknown activity type %s" % act["type"], activity_id=act["uri"], user_exception=UserException(UserExceptionType.Other)))
                    continue

                activity.CalculateUID()
                activities.append(activity)
            if not exhaustive or "next" not in res or not len(res["next"]):
                break
            else:
                pageUri = res["next"]
        logger.debug("Writing back meta cache")
        cachedb.sporttracks_meta_cache.update({"ExternalID": serviceRecord.ExternalID}, {"ExternalID": serviceRecord.ExternalID, "Activities": [{"ActivityURI": k, "TZ": v} for k, v in activity_tz_cache.items()]}, upsert=True)
        return activities, exclusions

    def _downloadActivity(self, serviceRecord, activity, returnFirstLocation=False):
        activityURI = activity.ServiceData["ActivityURI"]
        headers = self._getAuthHeaders(serviceRecord)
        activityData = requests.get(activityURI, headers=headers)
        activityData = activityData.json()

        if "clock_duration" in activityData:
            activity.EndTime = activity.StartTime + timedelta(seconds=float(activityData["clock_duration"]))

        activity.Private = "sharing" in activityData and activityData["sharing"] != "public"

        activity.GPS = False # Gets set back if there is GPS data

        if "notes" in activityData:
            activity.Notes = activityData["notes"]

        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilojoules, value=float(activityData["calories"]))

        activity.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(activityData["elevation_gain"]) if "elevation_gain" in activityData else None, loss=float(activityData["elevation_loss"]) if "elevation_loss" in activityData else None)

        activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=activityData["avg_heartrate"] if "avg_heartrate" in activityData else None, max=activityData["max_heartrate"] if "max_heartrate" in activityData else None)
        activity.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=activityData["avg_cadence"] if "avg_cadence" in activityData else None, max=activityData["max_cadence"] if "max_cadence" in activityData else None)
        activity.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=activityData["avg_power"] if "avg_power" in activityData else None, max=activityData["max_power"] if "max_power" in activityData else None)

        laps_info = []
        laps_starts = []
        if "laps" in activityData:
            laps_info = activityData["laps"]
            for lap in activityData["laps"]:
                laps_starts.append(dateutil.parser.parse(lap["start_time"]))
        lap = None
        for lapinfo in laps_info:
            lap = Lap()
            activity.Laps.append(lap)
            lap.StartTime = dateutil.parser.parse(lapinfo["start_time"])
            lap.EndTime = lap.StartTime + timedelta(seconds=lapinfo["clock_duration"])
            if "type" in lapinfo:
                lap.Intensity = LapIntensity.Active if lapinfo["type"] == "ACTIVE" else LapIntensity.Rest
            if "distance" in lapinfo:
                lap.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=float(lapinfo["distance"]))
            if "duration" in lapinfo:
                lap.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=lapinfo["duration"])
            if "calories" in lapinfo:
                lap.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilojoules, value=lapinfo["calories"])
            if "elevation_gain" in lapinfo:
                lap.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(lapinfo["elevation_gain"])))
            if "elevation_loss" in lapinfo:
                lap.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, loss=float(lapinfo["elevation_loss"])))
            if "max_speed" in lapinfo:
                lap.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, max=float(lapinfo["max_speed"])))
            if "max_speed" in lapinfo:
                lap.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, max=float(lapinfo["max_speed"])))
            if "avg_speed" in lapinfo:
                lap.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=float(lapinfo["avg_speed"])))
            if "max_heartrate" in lapinfo:
                lap.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=float(lapinfo["max_heartrate"])))
            if "avg_heartrate" in lapinfo:
                lap.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(lapinfo["avg_heartrate"])))
        if lap is None: # No explicit laps => make one that encompasses the entire activity
            lap = Lap()
            activity.Laps.append(lap)
            lap.Stats = activity.Stats
            lap.StartTime = activity.StartTime
            lap.EndTime = activity.EndTime
        elif len(activity.Laps) == 1:
            activity.Stats.update(activity.Laps[0].Stats) # Lap stats have a bit more info generally.
            activity.Laps[0].Stats = activity.Stats

        timerStops = []
        if "timer_stops" in activityData:
            for stop in activityData["timer_stops"]:
                timerStops.append([dateutil.parser.parse(stop[0]), dateutil.parser.parse(stop[1])])

        def isInTimerStop(timestamp):
            for stop in timerStops:
                if timestamp >= stop[0] and timestamp < stop[1]:
                    return True
                if timestamp >= stop[1]:
                    return False
            return False

        # Collate the individual streams into our waypoints.
        # Global sample rate is variable - will pick the next nearest stream datapoint.
        # Resampling happens on a lookbehind basis - new values will only appear their timestamp has been reached/passed

        wasInPause = False
        currentLapIdx = 0
        lap = activity.Laps[currentLapIdx]

        streams = []
        for stream in ["location", "elevation", "heartrate", "power", "cadence", "distance"]:
            if stream in activityData:
                streams.append(stream)
        stream_indices = dict([(stream, -1) for stream in streams]) # -1 meaning the stream has yet to start
        stream_lengths = dict([(stream, len(activityData[stream])/2) for stream in streams])
        # Data comes as "stream":[timestamp,value,timestamp,value,...]
        stream_values = {}
        for stream in streams:
            values = []
            for x in range(0,int(len(activityData[stream])/2)):
                values.append((activityData[stream][x * 2], activityData[stream][x * 2 + 1]))
            stream_values[stream] = values

        currentOffset = 0

        def streamVal(stream):
            nonlocal stream_values, stream_indices
            return stream_values[stream][stream_indices[stream]][1]

        def hasStreamData(stream):
            nonlocal stream_indices, streams
            return stream in streams and stream_indices[stream] >= 0

        while True:
            advance_stream = None
            advance_offset = None
            for stream in streams:
                if stream_indices[stream] + 1 == stream_lengths[stream]:
                    continue # We're at the end - can't advance
                if advance_offset is None or stream_values[stream][stream_indices[stream] + 1][0] - currentOffset < advance_offset:
                    advance_offset = stream_values[stream][stream_indices[stream] + 1][0] - currentOffset
                    advance_stream = stream
            if not advance_stream:
                break # We've hit the end of every stream, stop
            # Advance streams sharing the current timestamp
            for stream in streams:
                if stream == advance_stream:
                    continue # For clarity, we increment this later
                if stream_indices[stream] + 1 == stream_lengths[stream]:
                    continue # We're at the end - can't advance
                if stream_values[stream][stream_indices[stream] + 1][0] == stream_values[advance_stream][stream_indices[advance_stream] + 1][0]:
                    stream_indices[stream] += 1
            stream_indices[advance_stream] += 1 # Advance the key stream for this waypoint
            currentOffset = stream_values[advance_stream][stream_indices[advance_stream]][0] # Update the current time offset

            waypoint = Waypoint(activity.StartTime + timedelta(seconds=currentOffset))

            if hasStreamData("location"):
                waypoint.Location = Location(streamVal("location")[0], streamVal("location")[1], None)
                activity.GPS = True
                if returnFirstLocation:
                    return waypoint.Location

            if hasStreamData("elevation"):
                if not waypoint.Location:
                    waypoint.Location = Location(None, None, None)
                waypoint.Location.Altitude = streamVal("elevation")

            if hasStreamData("heartrate"):
                waypoint.HR = streamVal("heartrate")

            if hasStreamData("power"):
                waypoint.Power = streamVal("power")

            if hasStreamData("cadence"):
                waypoint.Cadence = streamVal("cadence")

            if hasStreamData("distance"):
                waypoint.Distance = streamVal("distance")

            inPause = isInTimerStop(waypoint.Timestamp)
            waypoint.Type = WaypointType.Regular if not inPause else WaypointType.Pause
            if wasInPause and not inPause:
                waypoint.Type = WaypointType.Resume
            wasInPause = inPause

            # We only care if it's possible to start a new lap, i.e. there are more left
            if currentLapIdx + 1 < len(laps_starts):
                if laps_starts[currentLapIdx + 1] < waypoint.Timestamp:
                    # A new lap has started
                    currentLapIdx += 1
                    lap = activity.Laps[currentLapIdx]

            lap.Waypoints.append(waypoint)

        if returnFirstLocation:
            return None  # I guess there were no waypoints?
        if activity.CountTotalWaypoints():
            activity.GetFlatWaypoints()[0].Type = WaypointType.Start
            activity.GetFlatWaypoints()[-1].Type = WaypointType.End
            activity.Stationary = False
        else:
            activity.Stationary = True

        return activity

    def DownloadActivity(self, serviceRecord, activity):
        return self._downloadActivity(serviceRecord, activity)

    def UploadActivity(self, serviceRecord, activity):
        activityData = {}
        # Props to the SportTracks API people for seamlessly supprting activities with or without TZ data.
        activityData["start_time"] = activity.StartTime.isoformat()
        if activity.Name:
            activityData["name"] = activity.Name
        if activity.Notes:
            activityData["notes"] = activity.Notes
        activityData["sharing"] = "public" if not activity.Private else "private"
        activityData["type"] = self._reverseActivityMappings[activity.Type]

        def _resolveDuration(obj):
            if obj.Stats.TimerTime.Value is not None:
                return obj.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
            if obj.Stats.MovingTime.Value is not None:
                return obj.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
            return (obj.EndTime - obj.StartTime).total_seconds()

        def _mapStat(dict, key, val, naturalValue=False):
            if val is not None:
                if naturalValue:
                    val = round(val)
                dict[key] = val
        _mapStat(activityData, "clock_duration", (activity.EndTime - activity.StartTime).total_seconds())
        _mapStat(activityData, "duration", _resolveDuration(activity)) # This has to be set, otherwise all time shows up as "stopped" :(
        _mapStat(activityData, "total_distance", activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
        _mapStat(activityData, "calories", activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilojoules).Value, naturalValue=True)
        _mapStat(activityData, "elevation_gain", activity.Stats.Elevation.Gain)
        _mapStat(activityData, "elevation_loss", activity.Stats.Elevation.Loss)
        _mapStat(activityData, "max_speed", activity.Stats.Speed.Max)
        _mapStat(activityData, "avg_heartrate", activity.Stats.HR.Average)
        _mapStat(activityData, "max_heartrate", activity.Stats.HR.Max)
        _mapStat(activityData, "avg_cadence", activity.Stats.Cadence.Average)
        _mapStat(activityData, "max_cadence", activity.Stats.Cadence.Max)
        _mapStat(activityData, "avg_power", activity.Stats.Power.Average)
        _mapStat(activityData, "max_power", activity.Stats.Power.Max)

        activityData["laps"] = []
        lapNum = 0
        for lap in activity.Laps:
            lapNum += 1
            lapinfo = {
                "number": lapNum,
                "start_time": lap.StartTime.isoformat(),
                "type": "REST" if lap.Intensity == LapIntensity.Rest else "ACTIVE"
            }
            _mapStat(lapinfo, "clock_duration", (lap.EndTime - lap.StartTime).total_seconds()) # Required too.
            _mapStat(lapinfo, "duration", _resolveDuration(lap)) # This field is required for laps to be created.
            _mapStat(lapinfo, "distance", lap.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value) # Probably required.
            _mapStat(lapinfo, "calories", lap.Stats.Energy.asUnits(ActivityStatisticUnit.Kilojoules).Value, naturalValue=True)
            _mapStat(lapinfo, "elevation_gain", lap.Stats.Elevation.Gain)
            _mapStat(lapinfo, "elevation_loss", lap.Stats.Elevation.Loss)
            _mapStat(lapinfo, "max_speed", lap.Stats.Speed.Max)
            _mapStat(lapinfo, "avg_heartrate", lap.Stats.HR.Average)
            _mapStat(lapinfo, "max_heartrate", lap.Stats.HR.Max)

            activityData["laps"].append(lapinfo)
        if not activity.Stationary:
            timer_stops = []
            timer_stopped_at = None

            def stream_append(stream, wp, data):
                stream += [round((wp.Timestamp - activity.StartTime).total_seconds()), data]

            location_stream = []
            distance_stream = []
            elevation_stream = []
            heartrate_stream = []
            power_stream = []
            cadence_stream = []
            for lap in activity.Laps:
                for wp in lap.Waypoints:
                    if wp.Location and wp.Location.Latitude and wp.Location.Longitude:
                        stream_append(location_stream, wp, [wp.Location.Latitude, wp.Location.Longitude])
                    if wp.HR:
                        stream_append(heartrate_stream, wp, round(wp.HR))
                    if wp.Distance:
                        stream_append(distance_stream, wp, wp.Distance)
                    if wp.Cadence or wp.RunCadence:
                        stream_append(cadence_stream, wp, round(wp.Cadence) if wp.Cadence else round(wp.RunCadence))
                    if wp.Power:
                        stream_append(power_stream, wp, wp.Power)
                    if wp.Location and wp.Location.Altitude:
                        stream_append(elevation_stream, wp, wp.Location.Altitude)
                    if wp.Type == WaypointType.Pause and not timer_stopped_at:
                        timer_stopped_at = wp.Timestamp
                    if wp.Type != WaypointType.Pause and timer_stopped_at:
                        timer_stops.append([timer_stopped_at, wp.Timestamp])
                        timer_stopped_at = None

            activityData["elevation"] = elevation_stream
            activityData["heartrate"] = heartrate_stream
            activityData["power"] = power_stream
            activityData["cadence"] = cadence_stream
            activityData["distance"] = distance_stream
            activityData["location"] = location_stream
            activityData["timer_stops"] = [[y.isoformat() for y in x] for x in timer_stops]

        headers = self._getAuthHeaders(serviceRecord)
        headers.update({"Content-Type": "application/json"})
        upload_resp = requests.post(self.OpenFitEndpoint + "/fitnessActivities.json", data=json.dumps(activityData), headers=headers)
        if upload_resp.status_code != 200:
            if upload_resp.status_code == 401:
                raise APIException("ST.mobi trial expired", block=True, user_exception=UserException(UserExceptionType.AccountExpired, intervention_required=True))
            raise APIException("Unable to upload activity %s" % upload_resp.text)
        return upload_resp.json()["uris"][0]


