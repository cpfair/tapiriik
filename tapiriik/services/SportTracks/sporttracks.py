from tapiriik.settings import WEB_ROOT, SPORTTRACKS_OPENFIT_ENDPOINT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, LapIntensity, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.sessioncache import SessionCache
from django.core.urlresolvers import reverse
import pytz
from datetime import timedelta
import dateutil.parser
from dateutil.tz import tzutc
import requests
import json

import logging
logger = logging.getLogger(__name__)

class SportTracksService(ServiceBase):
    ID = "sporttracks"
    DisplayName = "SportTracks"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    OpenFitEndpoint = SPORTTRACKS_OPENFIT_ENDPOINT
    SupportsHR = True

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
        ActivityType.Other: "other"
    }

    SupportedActivities = list(_reverseActivityMappings.keys())

    _sessionCache = SessionCache(lifetime=timedelta(minutes=30), freshen_on_get=True)

    def _get_cookies(self, record=None, email=None, password=None):
        return self._get_cookies_and_uid(record, email, password)[0]

    def _get_cookies_and_uid(self, record=None, email=None, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
        params = {"username": email, "password": password}
        resp = requests.post(self.OpenFitEndpoint + "/user/login", data=json.dumps(params), allow_redirects=False, headers={"Accept": "application/json", "Content-Type": "application/json"})
        if resp.status_code != 200:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        retval = (resp.cookies, int(resp.json()["user"]["uid"]))
        if record:
            self._sessionCache.Set(record.ExternalID, retval)
        return retval

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        cookies, uid = self._get_cookies_and_uid(email=email, password=password)
        return (uid, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        cookies = self._get_cookies(record=serviceRecord)
        activities = []
        exclusions = []
        pageUri = self.OpenFitEndpoint + "/fitnessActivities.json"
        while True:
            logger.debug("Req against " + pageUri)
            res = requests.get(pageUri, cookies=cookies)
            res = res.json()
            for act in res["items"]:
                activity = UploadedActivity()
                activity.ServiceData = {"ActivityURI": act["uri"]}

                if len(act["name"].strip()):
                    activity.Name = act["name"]
                activity.StartTime = dateutil.parser.parse(act["start_time"])
                if isinstance(activity.StartTime.tzinfo, tzutc):
                    activity.TZ = pytz.utc # The dateutil tzutc doesn't have an _offset value.
                else:
                    activity.TZ = pytz.FixedOffset(activity.StartTime.tzinfo._offset.total_seconds() / 60)  # Convert the dateutil lame timezones into pytz awesome timezones.

                activity.StartTime = activity.StartTime.replace(tzinfo=activity.TZ)
                activity.EndTime = activity.StartTime + timedelta(seconds=float(act["duration"]))
                activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=float(act["duration"])))  # OpenFit says this excludes paused times.

                # Sometimes activities get returned with a UTC timezone even when they are clearly not in UTC.
                if activity.TZ == pytz.utc:
                    # So, we get the first location in the activity and calculate the TZ from that.
                    try:
                        firstLocation = self._downloadActivity(serviceRecord, activity, returnFirstLocation=True)
                    except APIExcludeActivity:
                        pass
                    else:
                        activity.CalculateTZ(firstLocation)
                        activity.AdjustTZ()

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
                    exclusions.append(APIExcludeActivity("Unknown activity type %s" % act["type"], activityId=act["uri"]))
                    continue

                activity.CalculateUID()
                activities.append(activity)
            if not exhaustive or "next" not in res or not len(res["next"]):
                break
            else:
                pageUri = res["next"]
        return activities, exclusions

    def _downloadActivity(self, serviceRecord, activity, returnFirstLocation=False):
        activityURI = activity.ServiceData["ActivityURI"]
        cookies = self._get_cookies(record=serviceRecord)
        activityData = requests.get(activityURI, cookies=cookies)
        activityData = activityData.json()

        if "clock_duration" in activityData:
            activity.EndTime = activity.StartTime + timedelta(seconds=float(activityData["clock_duration"]))

        activity.Private = "sharing" in activityData and activityData["sharing"] != "public"

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
                lap.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=lapinfo["duration"]))
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

        if "location" not in activityData:
            activity.Stationary = True
        else:
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
            # Everything is resampled by nearest-neighbour to the rate of the location stream.
            parallel_indices = {}
            parallel_stream_lengths = {}
            for secondary_stream in ["elevation", "heartrate", "power", "cadence", "distance"]:
                if secondary_stream in activityData:
                    parallel_indices[secondary_stream] = 0
                    parallel_stream_lengths[secondary_stream] = len(activityData[secondary_stream])

            wasInPause = False
            currentLapIdx = 0
            lap = activity.Laps[currentLapIdx]
            for idx in range(0, len(activityData["location"]), 2):
                # Pick the nearest indices in the parallel streams
                for parallel_stream, parallel_index in parallel_indices.items():
                    if parallel_index + 2 == parallel_stream_lengths[parallel_stream]:
                        continue  # We're at the end of this stream
                    # Is the next datapoint a better choice than the current?
                    if abs(activityData["location"][idx] - activityData[parallel_stream][parallel_index + 2]) < abs(activityData["location"][idx] - activityData[parallel_stream][parallel_index]):
                        parallel_indices[parallel_stream] += 2

                waypoint = Waypoint(activity.StartTime + timedelta(0, activityData["location"][idx]))
                waypoint.Location = Location(activityData["location"][idx+1][0], activityData["location"][idx+1][1], None)
                if "elevation" in parallel_indices:
                    waypoint.Location.Altitude = activityData["elevation"][parallel_indices["elevation"]+1]

                if returnFirstLocation:
                    return waypoint.Location

                if "heartrate" in parallel_indices:
                    waypoint.HR = activityData["heartrate"][parallel_indices["heartrate"]+1]

                if "power" in parallel_indices:
                    waypoint.Power = activityData["power"][parallel_indices["power"]+1]

                if "cadence" in parallel_indices:
                    waypoint.Cadence = activityData["cadence"][parallel_indices["cadence"]+1]

                if "distance" in parallel_indices:
                    waypoint.Distance = activityData["distance"][parallel_indices["distance"]+1]

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
                activity.Laps[0].Waypoints[0].Type = WaypointType.Start
                activity.Laps[-1].Waypoints[-1].Type = WaypointType.End
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        return self._downloadActivity(serviceRecord, activity)

    def UploadActivity(self, serviceRecord, activity):
        activity.EnsureTZ()
        activityData = {}
        # Props to the SportTracks API people for seamlessly supprting activities with or without TZ data.
        activityData["start_time"] = activity.StartTime.isoformat()
        if activity.Name:
            activityData["name"] = activity.Name
        if activity.Notes:
            activityData["notes"] = activity.Notes
        activityData["sharing"] = "public" if not activity.Private else "private"
        activityData["type"] = self._reverseActivityMappings[activity.Type]

        def _mapStat(dict, key, val):
            if val is not None:
                dict[key] = val

        _mapStat(activityData, "clock_duration", (activity.EndTime - activity.StartTime).total_seconds())
        _mapStat(activityData, "duration", activity.Stats.MovingTime.Value.total_seconds() if activity.Stats.MovingTime.Value is not None else None)
        _mapStat(activityData, "total_distance", activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
        _mapStat(activityData, "calories", int(activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilojoules).Value))
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
            _mapStat(lapinfo, "clock_duration", (lap.EndTime - lap.StartTime).total_seconds())
            _mapStat(lapinfo, "duration", lap.Stats.MovingTime.Value.total_seconds() if lap.Stats.MovingTime.Value is not None else None)
            _mapStat(lapinfo, "distance", lap.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
            _mapStat(lapinfo, "calories", int(lap.Stats.Energy.asUnits(ActivityStatisticUnit.Kilojoules).Value))
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
                stream += [int((wp.Timestamp - activity.StartTime).total_seconds()), data]

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
                        stream_append(heartrate_stream, wp, int(wp.HR))
                    if wp.Distance:
                        stream_append(distance_stream, wp, wp.Distance)
                    if wp.Cadence or wp.RunCadence:
                        stream_append(cadence_stream, wp, int(wp.Cadence) if wp.Cadence else int(wp.RunCadence))
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

        cookies = self._get_cookies(record=serviceRecord)
        upload_resp = requests.post(self.OpenFitEndpoint + "/fitnessActivities.json", data=json.dumps(activityData), cookies=cookies, headers={"Content-Type": "application/json"})
        if upload_resp.status_code != 200:
            if upload_resp.status_code == 401:
                raise APIException("ST.mobi trial expired", block=True, user_exception=UserException(UserExceptionType.AccountExpired, intervention_required=True))
            raise APIException("Unable to upload activity %s" % upload_resp.text)


