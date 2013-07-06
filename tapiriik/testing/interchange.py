from unittest import TestCase

from tapiriik.testing.testtools import TestTools, TapiriikTestCase

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType, Waypoint, WaypointType
from tapiriik.sync import Sync

from datetime import datetime, timedelta
import random


class InterchangeTests(TapiriikTestCase):

    def test_round_precise_time(self):
        ''' Some services might return really exact times, while others would round to the second - needs to be accounted for in hash algo '''
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actB = Activity()
        actB.StartTime = datetime(1, 2, 3, 4, 5, 6, 7) + timedelta(0, 0.1337)

        actA.CalculateUID()
        actB.CalculateUID()

        self.assertEqual(actA.UID, actB.UID)

    def test_constant_representation(self):
        ''' ensures that all services' API clients are consistent through a simulated download->upload cycle '''
        #  runkeeper
        rkSvc = Service.FromID("runkeeper")
        act = TestTools.create_random_activity(rkSvc, rkSvc.SupportedActivities[0])
        record = rkSvc._createUploadData(act)
        returnedAct = rkSvc._populateActivity(record)
        act.Name = None  # RK doesn't have a "name" field, so it's fudged into the notes, but not really
        rkSvc._populateActivityWaypoints(record, returnedAct)
        self.assertActivitiesEqual(returnedAct, act)

        #  can't test Strava well this way, the upload and download formats are entirely different

        #  endomondo - only waypoints at this point, the activity metadata is somewhat out-of-band
        eSvc = Service.FromID("endomondo")

        act = TestTools.create_random_activity(eSvc, eSvc.SupportedActivities[0])
        oldWaypoints = act.Waypoints
        self.assertEqual(oldWaypoints[0].Calories, None)
        record = eSvc._createUploadData(act)
        eSvc._populateActivityFromTrackData(act, record)
        self.assertEqual(oldWaypoints, act.Waypoints)

    def test_duration_calculation(self):
        ''' ensures that true-duration calculation is being reasonable '''
        act = TestTools.create_blank_activity()
        act.StartTime = datetime.now()
        act.EndTime = act.StartTime + timedelta(hours=3)

        # No waypoints
        self.assertEqual(act.GetDuration(), timedelta(hours=3))

        # Too few waypoints
        act.Waypoints = [Waypoint(timestamp=act.StartTime), Waypoint(timestamp=act.EndTime)]
        self.assertEqual(act.GetDuration(), timedelta(hours=3))

        # straight-up calculation
        act.EndTime = act.StartTime + timedelta(seconds=14)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=10)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=14))]
        self.assertEqual(act.GetDuration(), timedelta(seconds=14))

        # pauses
        act.EndTime = act.StartTime + timedelta(seconds=14)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=9), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=10), ptType=WaypointType.Resume),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=14))]
        self.assertEqual(act.GetDuration(), timedelta(seconds=10))

        # laps - NO effect
        act.EndTime = act.StartTime + timedelta(seconds=14)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6), ptType=WaypointType.Lap),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=9)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=10), ptType=WaypointType.Lap),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=14))]
        self.assertEqual(act.GetDuration(), timedelta(seconds=14))

        # multiple pauses + ending after pause
        act.EndTime = act.StartTime + timedelta(seconds=20)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=9), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=10), ptType=WaypointType.Resume),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=12)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=16)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=17), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=20), ptType=WaypointType.End)]
        self.assertEqual(act.GetDuration(), timedelta(seconds=13))

        # implicit pauses (>11s)
        act.EndTime = act.StartTime + timedelta(seconds=20)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=20)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=24)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=30))]
        self.assertEqual(act.GetDuration(), timedelta(seconds=16))

        # mixed pauses - would this ever happen?? Either way, the explicit pause should override the implicit one and cause otherwise-ignored time to be counted
        act.EndTime = act.StartTime + timedelta(seconds=23)
        act.Waypoints = [Waypoint(timestamp=act.StartTime),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=2)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=6)),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=20), ptType=WaypointType.Pause),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=24), ptType=WaypointType.Resume),
                         Waypoint(timestamp=act.StartTime + timedelta(seconds=30))]
        self.assertEqual(act.GetDuration(), timedelta(seconds=26))

    def test_activity_specificity_resolution(self):
        # Mountain biking is more specific than just cycling
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.Cycling, ActivityType.MountainBiking]), ActivityType.MountainBiking)

        # But not once we mix in an unrelated activity - pick the first
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.Cycling, ActivityType.MountainBiking, ActivityType.Swimming]), ActivityType.Cycling)

        # Duplicates
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.Cycling, ActivityType.MountainBiking, ActivityType.MountainBiking]), ActivityType.MountainBiking)

        # One
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.MountainBiking]), ActivityType.MountainBiking)

        # With None
        self.assertEqual(ActivityType.PickMostSpecific([None, ActivityType.MountainBiking]), ActivityType.MountainBiking)

        # All None
        self.assertEqual(ActivityType.PickMostSpecific([None, None]), ActivityType.Other)

        # Never pick 'Other' given a better option
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.Other, ActivityType.MountainBiking]), ActivityType.MountainBiking)

        # Normal w/ Other + None
        self.assertEqual(ActivityType.PickMostSpecific([ActivityType.Other, ActivityType.Cycling, None, ActivityType.MountainBiking]), ActivityType.MountainBiking)
