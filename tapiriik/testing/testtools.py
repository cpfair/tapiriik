from unittest import TestCase

from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType, Waypoint, WaypointType, Location

from datetime import datetime, timedelta
import random
import pytz


class MockServiceA:
    ID = "mockA"
    SupportedActivities = [ActivityType.Rowing]


class MockServiceB:
    ID = "mockB"
    SupportedActivities = [ActivityType.Rowing, ActivityType.Wheelchair]


class TapiriikTestCase(TestCase):
    def assertActivitiesEqual(self, a, b):
        ''' compare activity records with more granular asserts '''
        if a == b:
            return
        else:
            self.assertEqual(a.StartTime, b.StartTime)
            self.assertEqual(a.EndTime, b.EndTime)
            self.assertEqual(a.Type, b.Type)
            self.assertEqual(a.Distance, b.Distance)
            self.assertEqual(a.Name, b.Name)
            self.assertEqual(len(a.Waypoints), len(b.Waypoints))
            for idx in range(0, len(a.Waypoints) - 1):
                self.assertEqual(a.Waypoints[idx].Timestamp.astimezone(pytz.utc), b.Waypoints[idx].Timestamp.astimezone(pytz.utc))
                self.assertEqual(a.Waypoints[idx].Location.Latitude, b.Waypoints[idx].Location.Latitude)
                self.assertEqual(a.Waypoints[idx].Location.Longitude, b.Waypoints[idx].Location.Longitude)
                self.assertEqual(a.Waypoints[idx].Location.Altitude, b.Waypoints[idx].Location.Altitude)
                self.assertEqual(a.Waypoints[idx].Type, b.Waypoints[idx].Type)
                self.assertEqual(a.Waypoints[idx].HR, b.Waypoints[idx].HR)
                self.assertEqual(a.Waypoints[idx].Calories, b.Waypoints[idx].Calories)
                self.assertEqual(a.Waypoints[idx].Power, b.Waypoints[idx].Power)
                self.assertEqual(a.Waypoints[idx].Cadence, b.Waypoints[idx].Cadence)
                self.assertEqual(a.Waypoints[idx].Temp, b.Waypoints[idx].Temp)

                self.assertEqual(a.Waypoints[idx].Location, b.Waypoints[idx].Location)
                self.assertEqual(a.Waypoints[idx], b.Waypoints[idx])
            self.assertEqual(a, b)


class TestTools:
    def create_mock_svc_record(svc):
        return {"Service": svc.ID}

    def create_mock_upload_record(svc):
        return {"ActivityID": random.randint(1, 1000), "Connection": TestTools.create_mock_svc_record(svc)}

    def create_random_activity(svc, actType=ActivityType.Other, tz=False):
        ''' creates completely random activity with valid waypoints and data '''
        act = Activity()

        if tz is True:
            tz = pytz.timezone(pytz.all_timezones[random.randint(0, len(pytz.all_timezones) - 1)])
            act.TZ = tz

        if len(act.Waypoints) > 0:
            raise ValueError("Waypoint list already populated")
        # this is entirely random in case the testing account already has events in it (API doesn't support delete, etc)
        act.StartTime = datetime(random.randint(2000, 2020), random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        if tz is not False:
            act.StartTime = tz.localize(act.StartTime)
        act.EndTime = act.StartTime + timedelta(0, random.randint(60 * 5, 60 * 60))  # don't really need to upload 1000s of pts to test this...
        act.Type = actType
        act.Distance = random.random() * 10000
        act.Name = str(random.random())
        paused = False
        waypointTime = act.StartTime
        backToBackPauses = False
        while waypointTime < act.EndTime:
            wp = Waypoint()
            if waypointTime == act.StartTime:
                wp.Type = WaypointType.Start
            wp.Timestamp = waypointTime
            wp.Location = Location(random.random() * 180 - 90, random.random() * 180 - 90, random.random() * 1000)  # this is gonna be one intense activity

            if not (wp.HR == wp.Cadence == wp.Calories == wp.Power == wp.Temp == None):
                raise ValueError("Waypoint did not initialize cleanly")
            if svc.SupportsHR:
                wp.HR = float(random.randint(90, 180))
            if svc.SupportsPower:
                wp.Power = float(random.randint(0, 1000))
            if svc.SupportsCalories:
                wp.Calories = float(random.randint(0, 500))
            if svc.SupportsCadence:
                wp.Cadence = float(random.randint(0, 100))
            if svc.SupportsTemp:
                wp.Temp = float(random.randint(0, 100))

            if (random.randint(40, 50) == 42 or backToBackPauses) and not paused:  # pause quite often
                wp.Type = WaypointType.Pause
                paused = True

            elif paused:
                paused = False
                wp.Type = WaypointType.Resume
                backToBackPauses = not backToBackPauses

            waypointTime += timedelta(0, int(random.random() + 9.5))  # 10ish seconds

            if waypointTime > act.EndTime:
                wp.Timestamp = act.EndTime
                wp.Type = WaypointType.End
            act.Waypoints.append(wp)
        if len(act.Waypoints) == 0:
            raise ValueError("No waypoints populated")
        return act

    def create_mock_services():
        mockA = MockServiceA()
        mockB = MockServiceB()
        Service._serviceMappings["mockA"] = mockA
        Service._serviceMappings["mockB"] = mockB
        return (mockA, mockB)
