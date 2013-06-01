from unittest import TestCase

from tapiriik.testing.testtools import TestTools, TapiriikTestCase

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType
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
        eSvc._populateActivityFromTrackRecord(act, record)
        self.assertEqual(oldWaypoints, act.Waypoints)
