from datetime import datetime, timedelta
import hashlib


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
    def __init__(self, startTime=datetime.min, endTime=datetime.min, actType=ActivityType.Other, waypointList=[]):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Type = actType
        self.Waypoints = waypointList

    def CalculateUID(self):
        csp = hashlib.new("md5")
        roundedStartTime = self.StartTime
        roundedStartTime = roundedStartTime - timedelta(microseconds=roundedStartTime.microsecond)
        csp.update(str(roundedStartTime).encode('utf-8'))
        self.UID = csp.hexdigest()

    def __str__(self):
        return "Activity (" + self.Type + ") Start " + str(self.StartTime) + " End " + str(self.EndTime)
    __repr__ = __str__


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity - not really merited


class WaypointType:
    Start = 0
    Regular = 1
    Pause = 11
    Resume = 12
    End = 100


class Waypoint:
    def __init__(self, timestamp=datetime.min, ptType=WaypointType.Regular, location=None, hr=None, power=None, calories=None):
        self.Timestamp = timestamp
        self.Location = location
        self.HR = hr
        self.Calories = calories
        self.Power = power  # I doubt there will ever be more parameters than this in terms of interchange
        self.Type = ptType

    def __str__(self):
        return "@" + str(self.Timestamp) + " " + str(self.Location.Latitude) + "|" + str(self.Location.Longitude) + "^" + str(round(self.Location.Altitude)) + " HR " + str(self.HR)
    __repr__ = __str__


class Location:
    def __init__(self, lat, lon, alt):
        self.Latitude = lat
        self.Longitude = lon
        self.Altitude = alt
        self.Datum = "WGS84"  # might eventually need to make this... better
