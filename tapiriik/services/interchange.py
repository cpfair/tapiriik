from datetime import datetime, timedelta
import requests
import hashlib
import pytz


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
    def __init__(self, startTime=datetime.min, endTime=datetime.min, actType=ActivityType.Other, distance=0, name=None, tz=None, waypointList=[]):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Type = actType
        self.Waypoints = waypointList
        self.Distance = distance
        self.TZ = tz
        self.Name = name

    def CalculateUID(self):
        csp = hashlib.new("md5")
        roundedStartTime = self.StartTime
        roundedStartTime = roundedStartTime - timedelta(microseconds=roundedStartTime.microsecond)
        csp.update(roundedStartTime.strftime("%Y-%m-%d %H:%M:%S").encode('utf-8'))  # exclude TZ for compat
        self.UID = csp.hexdigest()

    def DefineTZ(self):
        """ run localize() on all contained dates (doesn't change values) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        if self.StartTime.tzinfo is None:
            self.StartTime = self.TZ.localize(self.StartTime)
        if self.EndTime.tzinfo is None:
            self.EndTime = self.TZ.localize(self.EndTime)
        for wp in self.Waypoints:
            if wp.Timestamp.tzinfo is None:
                wp.Timestamp = self.TZ.localize(wp.Timestamp)

    def AdjustTZ(self):
        """ run astimezone() on all contained dates (requires non-naive DTs) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        self.StartTime = self.StartTime.astimezone(self.TZ)
        self.EndTime = self.EndTime.astimezone(self.TZ)

        for wp in self.Waypoints:
                wp.Timestamp = wp.Timestamp.astimezone(self.TZ)

    def CalculateTZ(self, loc=None):
        if self.TZ is not None:
            return self.TZ
        else:
            if len(self.Waypoints) == 0 and loc is None:
                raise Exception("Can't find TZ without waypoints")
            if loc is None:
                for wp in self.Waypoints:
                    if wp.Location is not None:
                        loc = wp.Location
                        break
                if loc is None:
                    raise Exception("Can't find TZ without a waypoint with a location")
            resp = requests.get("http://api.geonames.org/timezoneJSON?username=tapiriik&radius=0.5&lat=" + str(loc.Latitude) + "&lng=" + str(loc.Longitude))
            data = resp.json()
            tz = data["timezoneId"] if "timezoneId" in data else data["rawOffset"]
            self.TZ = pytz.timezone(tz)
            return self.TZ

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
        return "@" + str(self.Timestamp) + " " + str(self.Location.Latitude) + "|" + str(self.Location.Longitude) + "^" + str(round(self.Location.Altitude)) + " HR " + str(self.HR)
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
