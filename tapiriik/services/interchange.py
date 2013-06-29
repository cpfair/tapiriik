from datetime import datetime, timedelta
from tapiriik.database import cachedb
import requests
import hashlib
import pytz
import warnings


class ActivityType:  # taken from RK API docs. The text values have no meaning except for debugging
    Running = "Running"
    Cycling = "Cycling"
    MountainBiking = "MtnBiking"
    Walking = "Walking"
    Hiking = "Hiking"
    DownhillSkiing = "DownhillSkiing"
    CrossCountrySkiing = "XCSkiing"
    Snowboarding = "Snowboarding"
    Skating = "Skating"
    Swimming = "Swimming"
    Wheelchair = "Wheelchair"
    Rowing = "Rowing"
    Elliptical = "Elliptical"
    Other = "Other"

    # The right-most element is the "most specific."
    _hierarchy = [
        [Cycling, MountainBiking],
        [Running, Walking, Hiking]
    ]
    def PickMostSpecific(types):
        types = [x for x in types if x and x is not ActivityType.Other]
        if len(types) == 0:
            return ActivityType.Other
        most_specific = types[0]
        for definition in ActivityType._hierarchy:
            if len([x for x in types if x in definition]) == len(types):
                for act_type in types:
                    if definition.index(most_specific) < definition.index(act_type):
                        most_specific = act_type
        return most_specific


class Activity:
    ImplicitPauseTime = timedelta(minutes=1, seconds=5)

    def __init__(self, startTime=None, endTime=None, actType=ActivityType.Other, distance=None, name=None, tz=None, waypointList=None):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Type = actType
        self.Waypoints = waypointList if waypointList is not None else []
        self.Distance = distance
        self.TZ = tz
        self.Name = name

    def CalculateUID(self):
        if not self.StartTime:
            return  # don't even try
        csp = hashlib.new("md5")
        roundedStartTime = self.StartTime
        roundedStartTime = roundedStartTime - timedelta(microseconds=roundedStartTime.microsecond)
        if self.TZ:
            roundedStartTime = roundedStartTime.astimezone(self.TZ)
        csp.update(roundedStartTime.strftime("%Y-%m-%d %H:%M:%S").encode('utf-8'))  # exclude TZ for compat
        self.UID = csp.hexdigest()

    def DefineTZ(self):
        """ run localize() on all contained dates (doesn't change values) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        if self.StartTime and self.StartTime.tzinfo is None:
            self.StartTime = self.TZ.localize(self.StartTime)
        if self.EndTime and self.EndTime.tzinfo is None:
            self.EndTime = self.TZ.localize(self.EndTime)
        for wp in self.Waypoints:
            if wp.Timestamp.tzinfo is None:
                wp.Timestamp = self.TZ.localize(wp.Timestamp)
        self.CalculateUID()

    def AdjustTZ(self):
        """ run astimezone() on all contained dates (requires non-naive DTs) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        self.StartTime = self.StartTime.astimezone(self.TZ)
        self.EndTime = self.EndTime.astimezone(self.TZ)

        for wp in self.Waypoints:
                wp.Timestamp = wp.Timestamp.astimezone(self.TZ)
        self.CalculateUID()

    def CalculateTZ(self, loc=None):
        if len(self.Waypoints) == 0 and loc is None:
            raise Exception("Can't find TZ without waypoints")
        if loc is None:
            for wp in self.Waypoints:
                if wp.Location is not None and wp.Location.Latitude is not None and wp.Location.Longitude is not None:
                    loc = wp.Location
                    break
            if loc is None:
                raise Exception("Can't find TZ without a waypoint with a location")
        cachedTzData = cachedb.tz_cache.find_one({"Latitude": loc.Latitude, "Longitude": loc.Longitude})
        if cachedTzData is None:
            warnings.filterwarnings("ignore", "the 'strict' argument")
            warnings.filterwarnings("ignore", "unclosed <socket")
            resp = requests.get("http://api.geonames.org/timezoneJSON?username=tapiriik&radius=0.5&lat=" + str(loc.Latitude) + "&lng=" + str(loc.Longitude))
            data = resp.json()
            cachedTzData = {}
            if "timezoneId" in data:
                cachedTzData["TZ"] = data["timezoneId"]
            else:
                cachedTzData["TZ"] = data["rawOffset"]
            cachedTzData["Latitude"] = loc.Latitude
            cachedTzData["Longitude"] = loc.Longitude
            cachedb.tz_cache.insert(cachedTzData)

        if type(cachedTzData["TZ"]) != str:
            self.TZ = pytz.FixedOffset(cachedTzData["TZ"] * 60)
        else:
            self.TZ = pytz.timezone(cachedTzData["TZ"])
        return self.TZ

    def EnsureTZ(self):
        self.CalculateTZ()
        if self.StartTime.tzinfo is None:
            self.DefineTZ()
        else:
            self.AdjustTZ()

    def CalculateDistance(self):
        self.Distance = self.GetDistance()

    def GetDistance(self, startWpt=None, endWpt=None):
        import math
        dist = 0
        altHold = None  # seperate from the lastLoc variable, since we want to hold the altitude as long as required
        lastTimestamp = lastLoc = None

        if not startWpt:
            startWpt = self.Waypoints[0]
        if not endWpt:
            endWpt = self.Waypoints[-1]

        for x in range(self.Waypoints.index(startWpt), self.Waypoints.index(endWpt) + 1):
            timeDelta = self.Waypoints[x].Timestamp - lastTimestamp if lastTimestamp else None
            lastTimestamp = self.Waypoints[x].Timestamp

            if self.Waypoints[x].Type == WaypointType.Pause or (timeDelta and timeDelta > self.ImplicitPauseTime):
                lastLoc = None  # don't count distance while paused
                continue

            loc = self.Waypoints[x].Location
            if loc is None or loc.Longitude is None or loc.Latitude is None:
                # Used to throw an exception in this case, but the TCX schema allows for location-free waypoints, so we'll just patch over it.
                continue

            if loc and lastLoc:
                altHold = lastLoc.Altitude if lastLoc.Altitude is not None else altHold
                latRads = loc.Latitude * math.pi / 180
                meters_lat_degree = 1000 * 111.13292 + 1.175 * math.cos(4 * latRads) - 559.82 * math.cos(2 * latRads)
                meters_lon_degree = 1000 * 111.41284 * math.cos(latRads) - 93.5 * math.cos(3 * latRads)
                dx = (loc.Longitude - lastLoc.Longitude) * meters_lon_degree
                dy = (loc.Latitude - lastLoc.Latitude) * meters_lat_degree
                if loc.Altitude is not None and altHold is not None:  # incorporate the altitude when possible
                    dz = loc.Altitude - altHold
                else:
                    dz = 0
                dist += math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
            lastLoc = loc

        return dist

    def GetDuration(self, startWpt=None, endWpt=None):
        if len(self.Waypoints) < 3:
            # Either no waypoints, or one at the start and one at the end - just use regular time elapsed
            return self.EndTime - self.StartTime
        duration = timedelta(0)
        if not startWpt:
            startWpt = self.Waypoints[0]
        if not endWpt:
            endWpt = self.Waypoints[-1]
        lastTimestamp = None
        for x in range(self.Waypoints.index(startWpt), self.Waypoints.index(endWpt) + 1):
            wpt = self.Waypoints[x]
            delta = wpt.Timestamp - lastTimestamp if lastTimestamp else None
            lastTimestamp = wpt.Timestamp
            if wpt.Type is WaypointType.Pause:
                lastTimestamp = None
            elif delta and delta > self.ImplicitPauseTime:
                delta = None  # Implicit pauses
            if delta:
                duration += delta
        if duration.total_seconds() == 0:
            raise ValueError("Zero-duration activity")
        return duration

    def CheckSanity(self):
        if not hasattr(self, "UploadedTo") or len(self.UploadedTo) == 0:
            raise ValueError("Unset UploadedTo field")
        srcs = self.UploadedTo  # this is just so I can see the source of the activity in the exception message
        if self.TZ and self.TZ.utcoffset(self.StartTime.replace(tzinfo=None)) != self.StartTime.tzinfo.utcoffset(self.StartTime.replace(tzinfo=None)):
            raise ValueError("Inconsistent timezone between StartTime (" + str(self.StartTime) + ") and activity (" + str(self.TZ) + ")")
        if self.TZ and self.TZ.utcoffset(self.EndTime.replace(tzinfo=None)) != self.StartTime.tzinfo.utcoffset(self.EndTime.replace(tzinfo=None)):
            raise ValueError("Inconsistent timezone between EndTime (" + str(self.EndTime) + ") and activity (" + str(self.TZ) + ")")
        if len(self.Waypoints) == 0:
            raise ValueError("No waypoints")
        if len(self.Waypoints) == 1:
            raise ValueError("Only one waypoint")
        if self.Distance is not None and self.Distance > 1000 * 1000:
            raise ValueError("Exceedingly long activity (distance)")
        if self.StartTime and self.EndTime:
            # We can only do these checks if the activity has both start and end times (Dropbox)
            if (self.EndTime - self.StartTime).total_seconds() < 0:
                raise ValueError("Event finishes before it starts")
            if (self.EndTime - self.StartTime).total_seconds() == 0:
                raise ValueError("0-duration activity")
            if (self.EndTime - self.StartTime).total_seconds() > 60 * 60 * 24 * 5:
                raise ValueError("Exceedingly long activity (time)")
        altLow = None
        altHigh = None
        pointsWithoutLocation = 0
        for wp in self.Waypoints:
            if self.TZ and self.TZ.utcoffset(wp.Timestamp.replace(tzinfo=None)) != wp.Timestamp.tzinfo.utcoffset(wp.Timestamp.replace(tzinfo=None)):
                raise ValueError("WP " + str(wp.Timestamp) + " and activity timezone (" + str(self.TZ) + ") are inconsistent")
            if wp.Location:
                if wp.Location.Latitude == 0 and wp.Location.Longitude == 0:
                    raise ValueError("Invalid lat/lng")
                if wp.Location.Altitude is not None and (altLow is None or wp.Location.Altitude < altLow):
                    altLow = wp.Location.Altitude
                if wp.Location.Altitude is not None and (altHigh is None or wp.Location.Altitude > altHigh):
                    altHigh = wp.Location.Altitude
            if not wp.Location or wp.Location.Latitude is None or wp.Location.Longitude is None:
                pointsWithoutLocation += 1
        if pointsWithoutLocation == len(self.Waypoints):
            raise ValueError("No points have location")
        if altLow is not None and altLow == altHigh and altLow == 0:  # some activities have very sporadic altitude data, we'll let it be...
            raise ValueError("Invalid altitudes / no change from " + str(altLow))

    def __str__(self):
        return "Activity (" + self.Type + ") Start " + str(self.StartTime) + " " + str(self.StartTime.tzinfo if self.StartTime else "") + " End " + str(self.EndTime) + " " + str(len(self.Waypoints)) + " WPs"
    __repr__ = __str__

    def __eq__(self, other):
        # might need to fix this for TZs?
        return self.StartTime == other.StartTime and self.EndTime == other.EndTime and self.Type == other.Type and self.Waypoints == other.Waypoints and self.Distance == other.Distance and self.Name == other.Name

    def __ne__(self, other):
        return not self.__eq__(other)


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity - not really merited


class WaypointType:
    Start = 0   # Start of activity
    Regular = 1 # Normal
    Lap = 2     # A new lap starts with this
    Pause = 11  # All waypoints within a paused period should have this type
    Resume = 12 # The first waypoint after a paused period
    End = 100   # End of activity


class Waypoint:
    def __init__(self, timestamp=None, ptType=WaypointType.Regular, location=None, hr=None, power=None, calories=None, cadence=None, temp=None):
        self.Timestamp = timestamp
        self.Location = location
        self.HR = hr
        self.Calories = calories
        self.Power = power  # I doubt there will ever be more parameters than this in terms of interchange
        self.Temp = temp  # never say never
        self.Cadence = cadence  # dammit this better be the last one
        self.Type = ptType

    def __eq__(self, other):
        return self.Timestamp == other.Timestamp and self.Location == other.Location and self.HR == other.HR and self.Calories == other.Calories and self.Temp == other.Temp and self.Cadence == other.Cadence and self.Type == other.Type and self.Power == other.Power

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        if self.Location is None:
            return str(self.Type)+"@"+str(self.Timestamp)
        return str(self.Type) + "@" + str(self.Timestamp) + " " + str(self.Location.Latitude) + "|" + str(self.Location.Longitude) + "^" + str(round(self.Location.Altitude) if self.Location.Altitude is not None else None) + "\n\tHR " + str(self.HR) + " CAD " + str(self.Cadence) + " TEMP " + str(self.Temp) + " PWR " + str(self.Power) + " CAL " + str(self.Calories)
    __repr__ = __str__


class Location:
    def __init__(self, lat, lon, alt):
        self.Latitude = lat
        self.Longitude = lon
        self.Altitude = alt
        self.Datum = "WGS84"  # might eventually need to make this... better

    def __eq__(self, other):
        return self.Latitude == other.Latitude and self.Longitude == other.Longitude and self.Altitude == other.Altitude and self.Datum == other.Datum

    def __ne__(self, other):
        return not self.__eq__(other)
