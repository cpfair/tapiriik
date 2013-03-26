from unittest import TestCase

from tapiriik.testing.testtools import TestTools

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType
from tapiriik.sync import Sync

from datetime import datetime, timedelta
import random


class SyncTests(TestCase):

    def test_svc_level_dupe(self):
        ''' check that service-level duplicate activities are caught (no DB involvement) '''
        svcA, svcB = TestTools.create_mock_services()
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.UploadedTo = [TestTools.create_mock_upload_record(svcA)]
        actB = Activity()
        actB.StartTime = actA.StartTime
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]

        actA.CalculateUID()
        actB.CalculateUID()

        activities = []
        Sync._accumulateActivities(Service.FromID("mockA"), [actA], activities)
        Sync._accumulateActivities(Service.FromID("mockB"), [actB], activities)

        self.assertEqual(len(activities), 1)

    def test_svc_level_dupe_time_leeway(self):
        ''' check that service-level duplicate activities within the defined time leeway are caught '''
        svcA, svcB = TestTools.create_mock_services()
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.UploadedTo = [TestTools.create_mock_upload_record(svcA)]
        actA.Type = set(svcA.SupportedActivities).intersection(set(svcB.SupportedActivities)).pop()
        actB = Activity()
        actB.StartTime = datetime(1, 2, 3, 4, 6, 6, 7)
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]
        actB.Type = actA.Type

        actA.CalculateUID()
        actB.CalculateUID()

        activities = []
        Sync._accumulateActivities(Service.FromID("mockA"), [actA], activities)
        Sync._accumulateActivities(Service.FromID("mockB"), [actB], activities)

        self.assertIn(actA.UID, actA.UIDs)
        self.assertIn(actB.UID, actA.UIDs)
        self.assertIn(actA.UID, actB.UIDs)
        self.assertIn(actB.UID, actB.UIDs)

        # we need to fake up the service records to avoid having to call the actual sync method where these values are normally preset
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        recA["SynchronizedActivities"] = [actA.UID]
        recB["SynchronizedActivities"] = [actB.UID]

        recipientServicesA = Sync._determineRecipientServices(actA, [recA, recB])
        recipientServicesB = Sync._determineRecipientServices(actB, [recA, recB])

        self.assertEqual(len(recipientServicesA), 0)
        self.assertEqual(len(recipientServicesB), 0)
        self.assertEqual(len(activities), 1)

    def test_svc_supported_activity_types(self):
        ''' check that only activities are only sent to services which support them '''
        svcA, svcB = TestTools.create_mock_services()
        svcA.SupportedActivities = [ActivityType.CrossCountrySkiing]
        svcB.SupportedActivities = [ActivityType.Cycling]

        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.UploadedTo = [TestTools.create_mock_upload_record(svcA)]
        actA.Type = svcA.SupportedActivities[0]
        actB = Activity()
        actB.StartTime = datetime(5, 6, 7, 8, 9, 10, 11)
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]
        actB.Type = [x for x in svcB.SupportedActivities if x != actA.Type][0]

        actA.CalculateUID()
        actB.CalculateUID()

        allConns = [actA.UploadedTo[0]["Connection"], actB.UploadedTo[0]["Connection"]]

        activities = []
        Sync._accumulateActivities(svcA, [actA], activities)
        Sync._accumulateActivities(svcB, [actB], activities)

        syncToA = Sync._determineRecipientServices(actA, allConns)
        syncToB = Sync._determineRecipientServices(actB, allConns)

        self.assertEqual(len(syncToA), 0)
        self.assertEqual(len(syncToB), 0)

        svcB.SupportedActivities = svcA.SupportedActivities

        syncToA = Sync._determineRecipientServices(actA, allConns)
        syncToB = Sync._determineRecipientServices(actB, allConns)

        self.assertEqual(len(syncToA), 1)
        self.assertEqual(len(syncToB), 0)

        svcB.SupportedActivities = svcA.SupportedActivities = [ActivityType.CrossCountrySkiing, ActivityType.Cycling]

        syncToA = Sync._determineRecipientServices(actA, allConns)
        syncToB = Sync._determineRecipientServices(actB, allConns)

        self.assertEqual(len(syncToA), 1)
        self.assertEqual(len(syncToB), 1)
