from unittest import TestCase

from tapiriik.testing.testtools import TestTools

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType
from tapiriik.sync import Sync

from datetime import datetime, timedelta
import random


class InterchangeTests(TestCase):

    def test_round_precise_time(self):
        ''' Some services might return really exact times, while others would round to the second - needs to be accounted for in hash algo '''
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actB = Activity()
        actB.StartTime = datetime(1, 2, 3, 4, 5, 6, 7) + timedelta(0, random.random() / 2)

        actA.CalculateUID()
        actB.CalculateUID()

        self.assertEqual(actA.UID, actB.UID)
