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


class Activity:
    def __init__(self, startTime=datetime.min, endTime=datetime.min, actType=ActivityType.Other, distance=None, name=None, tz=None, waypointList=None):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Type = actType
        self.Waypoints = waypointList if waypointList is not None else []
        self.Distance = distance
        self.TZ = tz
        self.Name = name

    def CalculateUID(self):
        if self.StartTime is datetime.min:
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
        if self.StartTime.tzinfo is None and self.StartTime is not datetime.min:
            self.StartTime = self.TZ.localize(self.StartTime)
        if self.EndTime.tzinfo is None and self.EndTime is not datetime.min:
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
        import math
        dist = 0
        for x in range(1, len(self.Waypoints)):
            if self.Waypoints[x - 1].Type == WaypointType.Pause:
                continue  # don't count distance while paused
            lastLoc = self.Waypoints[x - 1].Location
            loc = self.Waypoints[x].Location
            latRads = loc.Latitude * math.pi / 180
            meters_lat_degree = 1000 * 111.13292 + 1.175 * math.cos(4 * latRads) - 559.82 * math.cos(2 * latRads)
            meters_lon_degree = 1000 * 111.41284 * math.cos(latRads) - 93.5 * math.cos(3 * latRads)
            dx = (loc.Longitude - lastLoc.Longitude) * meters_lon_degree
            dy = (loc.Latitude - lastLoc.Latitude) * meters_lat_degree
            dz = loc.Altitude - lastLoc.Altitude
            dist += math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
        self.Distance = dist

    def CheckSanity(self):
        if len(self.Waypoints) == 0:
            raise ValueError("No waypoints")
        if len(self.Waypoints) == 1:
            raise ValueError("Only one waypoint")
        if self.Distance is not None and self.Distance > 1000 * 1000:
            raise ValueError("Exceedlingly long activity (distance)")
        if (self.EndTime - self.StartTime).total_seconds() < 0:
            raise ValueError("Event finishes before it starts")
        if (self.EndTime - self.StartTime).total_seconds() == 0 and self.StartTime != datetime.min:
            # the 2nd condition here is for Dropbox - which cheats and just fills in the UID
            raise ValueError("0-duration activity")
        if (self.EndTime - self.StartTime).total_seconds() > 60 * 60 * 24 * 5:
            raise ValueError("Exceedlingly long activity (time)")

        altLow = None
        altHigh = None
        pointsWithoutLocation = 0
        for wp in self.Waypoints:
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
        if altLow is not None and altLow == altHigh:
            raise ValueError("Invalid altitudes / no change from " + str(altLow))

    def __str__(self):
        return "Activity (" + self.Type + ") Start " + str(self.StartTime) + " End " + str(self.EndTime)
    __repr__ = __str__

    def __eq__(self, other):
        # might need to fix this for TZs?
        return self.StartTime == other.StartTime and self.EndTime == other.EndTime and self.Type == other.Type and self.Waypoints == other.Waypoints and self.Distance == other.Distance and self.Name == other.Name

    def __ne__(self, other):
        return not self.__eq__(other)


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity - not really merited


class WaypointType:
    Start = 0
    Regular = 1
    Pause = 11
    Resume = 12
    End = 100


class Waypoint:
    def __init__(self, timestamp=datetime.min, ptType=WaypointType.Regular, location=None, hr=None, power=None, calories=None, cadence=None, temp=None):
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
