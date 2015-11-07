from unittest import TestCase

from tapiriik.services import Service, ServiceRecord, ServiceBase
from tapiriik.services.interchange import Activity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Lap, Location

from datetime import datetime, timedelta
import random
import pytz
from tapiriik.database import db


class MockServiceA(ServiceBase):
    ID = "mockA"
    SupportedActivities = [ActivityType.Rowing]


class MockServiceB(ServiceBase):
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
            self.assertEqual(a.Stats.Distance, b.Stats.Distance)
            self.assertEqual(a.Name, b.Name)
            self.assertLapsListsEqual(a.Laps, b.Laps)

    def assertLapsListsEqual(self, lapsa, lapsb):
        self.assertEqual(len(lapsa), len(lapsb))
        for idx in range(len(lapsa)):
            la = lapsa[idx]
            lb = lapsb[idx]
            self.assertLapsEqual(la, lb)

    def assertLapsEqual(self, la, lb):
        self.assertEqual(la.StartTime, lb.StartTime)
        self.assertEqual(la.EndTime, lb.EndTime)
        self.assertEqual(len(la.Waypoints), len(lb.Waypoints))
        for idx in range(len(la.Waypoints)):
            wpa = la.Waypoints[idx]
            wpb = lb.Waypoints[idx]
            self.assertEqual(wpa.Timestamp.astimezone(pytz.utc), wpb.Timestamp.astimezone(pytz.utc))
            self.assertEqual(wpa.Location.Latitude, wpb.Location.Latitude)
            self.assertEqual(wpa.Location.Longitude, wpb.Location.Longitude)
            self.assertEqual(wpa.Location.Altitude, wpb.Location.Altitude)
            self.assertEqual(wpa.Type, wpb.Type)
            self.assertEqual(wpa.HR, wpb.HR)
            self.assertEqual(wpa.Calories, wpb.Calories)
            self.assertEqual(wpa.Power, wpb.Power)
            self.assertEqual(wpa.Cadence, wpb.Cadence)
            self.assertEqual(wpa.Temp, wpb.Temp)
            self.assertEqual(wpa.Location, wpb.Location)
            self.assertEqual(wpa, wpb)


class TestTools:
    def create_mock_user():
        db.test.insert({"asd": "asdd"})
        return {"_id": str(random.randint(1, 1000))}

    def create_mock_svc_record(svc):
        return ServiceRecord({"Service": svc.ID, "_id": str(random.randint(1, 1000)), "ExternalID": str(random.randint(1, 1000))})

    def create_mock_servicedata(svc, record=None):
        return {"ActivityID": random.randint(1, 1000), "Connection": record}

    def create_mock_servicedatacollection(svc, record=None):
        record = record if record else TestTools.create_mock_svc_record(svc)
        return {record._id: TestTools.create_mock_servicedata(svc, record=record)}

    def create_blank_activity(svc=None, actType=ActivityType.Other, record=None):
        act = Activity()
        act.Type = actType
        if svc:
            record = record if record else TestTools.create_mock_svc_record(svc)
            act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svc, record=record)
        act.StartTime = datetime.now()
        act.EndTime = act.StartTime + timedelta(seconds=42)
        act.CalculateUID()
        return act

    def create_random_activity(svc=None, actType=ActivityType.Other, tz=False, record=None, withPauses=True, withLaps=True):
        ''' creates completely random activity with valid waypoints and data '''
        act = TestTools.create_blank_activity(svc, actType, record=record)

        if tz is True:
            tz = pytz.timezone("America/Atikokan")
            act.TZ = tz
        elif tz is not False:
            act.TZ = tz

        if act.CountTotalWaypoints() > 0:
            raise ValueError("Waypoint list already populated")
        # this is entirely random in case the testing account already has events in it (API doesn't support delete, etc)
        act.StartTime = datetime(2011, 12, 13, 14, 15, 16)
        if tz is not False:
            if hasattr(tz, "localize"):
                act.StartTime = tz.localize(act.StartTime)
            else:
                act.StartTime = act.StartTime.replace(tzinfo=tz)
        act.EndTime = act.StartTime + timedelta(0, random.randint(60 * 5, 60 * 60))  # don't really need to upload 1000s of pts to test this...
        act.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=random.random() * 10000)
        act.Name = str(random.random())
        paused = False
        waypointTime = act.StartTime
        backToBackPauses = False
        act.Laps = []
        lap = Lap(startTime=act.StartTime)
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

            if withPauses and (random.randint(40, 50) == 42 or backToBackPauses) and not paused:  # pause quite often
                wp.Type = WaypointType.Pause
                paused = True

            elif paused:
                paused = False
                wp.Type = WaypointType.Resume
                backToBackPauses = not backToBackPauses

            waypointTime += timedelta(0, int(random.random() + 9.5))  # 10ish seconds

            lap.Waypoints.append(wp)
            if waypointTime > act.EndTime:
                wp.Timestamp = act.EndTime
                wp.Type = WaypointType.End
            elif withLaps and wp.Timestamp < act.EndTime and random.randint(40, 60) == 42:
                # occasionally start new laps
                lap.EndTime = wp.Timestamp
                act.Laps.append(lap)
                lap = Lap(startTime=waypointTime)

        # Final lap
        lap.EndTime = act.EndTime
        act.Laps.append(lap)
        if act.CountTotalWaypoints() == 0:
            raise ValueError("No waypoints populated")

        act.CalculateUID()
        act.EnsureTZ()

        return act

    def create_mock_service(id):
        mock = MockServiceA()
        mock.ID = id
        Service._serviceMappings[id] = mock
        return mock

    def create_mock_services():
        mockA = MockServiceA()
        mockB = MockServiceB()
        Service._serviceMappings["mockA"] = mockA
        Service._serviceMappings["mockB"] = mockB
        return (mockA, mockB)
