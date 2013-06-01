from tapiriik.testing.testtools import TestTools, TapiriikTestCase

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType
from tapiriik.auth import User

from datetime import datetime, timedelta, tzinfo
import random
import pytz
import copy


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)


class SyncTests(TapiriikTestCase):

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

    def test_svc_level_dupe_tz_uniform(self):
        ''' check that service-level duplicate activities with the same TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        actA = Activity()
        actA.StartTime = pytz.timezone("America/Denver").localize(datetime(1, 2, 3, 4, 5, 6, 7))
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

    def test_svc_level_dupe_tz_nonuniform(self):
        ''' check that service-level duplicate activities with non-uniform TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.UploadedTo = [TestTools.create_mock_upload_record(svcA)]
        actB = Activity()
        actB.StartTime = pytz.timezone("America/Denver").localize(actA.StartTime)
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]

        actA.CalculateUID()
        actB.CalculateUID()

        activities = []
        Sync._accumulateActivities(Service.FromID("mockA"), [actA], activities)
        Sync._accumulateActivities(Service.FromID("mockB"), [actB], activities)

        self.assertEqual(len(activities), 1)

    def test_svc_level_dupe_tz_irregular(self):
        ''' check that service-level duplicate activities with irregular TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        actA = Activity()
        actA.StartTime = pytz.timezone("America/Edmonton").localize(datetime(1, 2, 3, 4, 5, 6, 7))
        actA.UploadedTo = [TestTools.create_mock_upload_record(svcA)]
        actB = Activity()
        actB.StartTime = actA.StartTime.astimezone(pytz.timezone("America/Iqaluit"))
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
        recA.SynchronizedActivities = [actA.UID]
        recB.SynchronizedActivities = [actB.UID]

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

    def test_activity_coalesce_normaltz(self):
        ''' ensure that we can't coalesce activities with non-pytz timezones '''
        svcA, svcB = TestTools.create_mock_services()
        actA = TestTools.create_random_activity(svcA, tz=UTC())

        actB = Activity()
        actB.StartTime = actA.StartTime.replace(tzinfo=None) + timedelta(seconds=10)
        actB.EndTime = actA.EndTime.replace(tzinfo=None)
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]
        actA.Name = "Not this"
        actB.Name = "Heya"
        actB.Type = ActivityType.Walking
        actA.CalculateUID()
        actB.CalculateUID()

        activities = []
        Sync._accumulateActivities(Service.FromID("mockB"), [copy.deepcopy(actB)], activities)
        self.assertRaises(ValueError, Sync._accumulateActivities, Service.FromID("mockA"), [copy.deepcopy(actA)], activities)


    def test_activity_coalesce(self):
        ''' ensure that activity data is getting coalesced by _accumulateActivities '''
        svcA, svcB = TestTools.create_mock_services()
        actA = TestTools.create_random_activity(svcA, tz=pytz.timezone("America/Iqaluit"))
        actB = Activity()
        actB.StartTime = actA.StartTime.replace(tzinfo=None)
        actB.UploadedTo = [TestTools.create_mock_upload_record(svcB)]
        actA.Name = "Not this"
        actB.Name = "Heya"
        actB.Type = ActivityType.Walking
        actA.CalculateUID()
        actB.CalculateUID()

        activities = []
        Sync._accumulateActivities(Service.FromID("mockB"), [copy.deepcopy(actB)], activities)
        Sync._accumulateActivities(Service.FromID("mockA"), [copy.deepcopy(actA)], activities)

        self.assertEqual(len(activities), 1)
        act = activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.Waypoints, actA.Waypoints)
        self.assertEqual(act.Name, actB.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Same here.
        self.assertTrue(actB.UploadedTo[0] in act.UploadedTo)
        self.assertTrue(actA.UploadedTo[0] in act.UploadedTo)

        activities = []
        Sync._accumulateActivities(Service.FromID("mockA"), [copy.deepcopy(actA)], activities)
        Sync._accumulateActivities(Service.FromID("mockB"), [copy.deepcopy(actB)], activities)

        self.assertEqual(len(activities), 1)
        act = activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.Waypoints, actA.Waypoints)
        self.assertEqual(act.Name, actA.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Exception: ActivityType.Other does not take priority
        self.assertTrue(actB.UploadedTo[0] in act.UploadedTo)
        self.assertTrue(actA.UploadedTo[0] in act.UploadedTo)

        actA.Type = ActivityType.CrossCountrySkiing
        activities = []
        Sync._accumulateActivities(Service.FromID("mockA"), [copy.deepcopy(actA)], activities)
        Sync._accumulateActivities(Service.FromID("mockB"), [copy.deepcopy(actB)], activities)

        self.assertEqual(len(activities), 1)
        act = activities[0]
        self.assertEqual(act.Type, actA.Type)  # Here, it will take priority.



    def test_eligibility_excluded(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        recipientServices = [recA, recB]
        excludedServices = [recA]
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recB in eligible)
        self.assertTrue(recA not in eligible)

    def test_eligibility_config(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        svcA.Configurable = True
        svcA.RequiresConfiguration = lambda x: True
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        recipientServices = [recA, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recB in eligible)
        self.assertTrue(recA not in eligible)

    def test_eligibility_flowexception(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recA)
        act.Origin = recA
        User.SetFlowException(user, recA, recB, flowToTarget=False)
        recipientServices = [recA, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA in eligible)
        self.assertFalse(recB in eligible)

    def test_eligibility_flowexception_reverse(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        User.SetFlowException(user, recA, recB, flowToSource=False)
        recipientServices = [recA, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertFalse(recA in eligible)
        self.assertTrue(recB in eligible)

    def test_eligibility_flowexception_both(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        recipientServices = [recA, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertFalse(recA in eligible)
        self.assertTrue(recB in eligible)

        act.Origin = recA
        act.UploadedTo = [TestTools.create_mock_upload_record(svcA, record=recA)]
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA in eligible)
        self.assertFalse(recB in eligible)

    def test_eligibility_flowexception_none(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        recipientServices = [recA]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.UploadedTo = [TestTools.create_mock_upload_record(svcA, record=recA)]
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

    def test_eligibility_flowexception_change(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB

        recipientServices = [recA]
        excludedServices = []


        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=True)
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.UploadedTo = [TestTools.create_mock_upload_record(svcA, record=recA)]
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=False)
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recA, recB]
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=True)
        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)

        eligible = Sync._determineEligibleRecipientServices(activity=act, recipientServices=recipientServices, excludedServices=excludedServices, user=user, silent=True)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)