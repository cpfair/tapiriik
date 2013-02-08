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
        rkSvc = Service.FromID("runkeeper")
        act = TestTools.create_random_activity(rkSvc, rkSvc.SupportedActivities[0])
        record = rkSvc._createUploadData(act)
        returnedAct = rkSvc._populateActivity(record)
        rkSvc._populateActivityWaypoints(record, act)

        self.assertActivitiesEqual(returnedAct, act)


