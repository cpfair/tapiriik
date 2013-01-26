from datetime import datetime


class Activity:
    def __init__(self, startTime=datetime.min, endTime=datetime.min, waypointList=[]):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Waypoints = waypointList


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity


class Waypoint:
    def __init__(self, timestamp=None, location=None, hr=None, power=None):
        self.Timestamp = timestamp or datetime.min
        self.Location = location
        self.HR = hr
        self.Power = power  # I doubt there will ever be more parameters than this in terms of interchange


class Location:
    def __init__(self, lat, lon):
        self.Latitude = lat
        self.Longitude = lon
        self.Datum = "WGS84"  # might eventually need to make this... better
