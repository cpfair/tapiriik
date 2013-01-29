from datetime import datetime
import hashlib


class Activity:
    def __init__(self, startTime=datetime.min, endTime=datetime.min, waypointList=[]):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Waypoints = waypointList

    def CalculateUID(self):
        csp = hashlib.new("md5")
        csp.update(str(self.StartTime).encode('utf-8'))
        self.UID = csp.hexdigest()

    def __str__(self):
        return "Activity Start " + str(self.StartTime) + " End " + str(self.EndTime)
    __repr__ = __str__


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity - not really merited


class WaypointType:
    Start = 0
    Regular = 1
    Pause = 11
    Resume = 12
    End = 100


class WaypointType:
    Start = 0
    Regular = 1
    Pause = 11
    Resume = 12
    End = 100


class Waypoint:
    def __init__(self, timestamp=None, type=None, location=None, hr=None, power=None):
        self.Timestamp = timestamp or datetime.min
        self.Location = location
        self.HR = hr
        self.Power = power  # I doubt there will ever be more parameters than this in terms of interchange
        self.Type = type or WaypointType.Regular

    def __str__(self):
        return "@" + str(self.Timestamp) + " " + str(self.Location.Latitude) + "|" + str(self.Location.Longitude) + "^" + str(round(self.Location.Altitude)) + " HR " + str(self.HR)
    __repr__ = __str__


class Location:
    def __init__(self, lat, lon, alt):
        self.Latitude = lat
        self.Longitude = lon
        self.Altitude = alt
        self.Datum = "WGS84"  # might eventually need to make this... better
