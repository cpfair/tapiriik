from tapiriik.testing.testtools import TestTools, TapiriikTestCase

from tapiriik.sync import SynchronizationTask
from tapiriik.sync.activity_record import ActivityRecord
from tapiriik.services import UserException, UserExceptionType
from tapiriik.services.api import APIExcludeActivity
from tapiriik.services.interchange import Activity, ActivityType
from tapiriik.auth import User

from datetime import datetime, timedelta, tzinfo
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
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)

        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actB = Activity()
        actB.StartTime = actA.StartTime
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)

        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []

        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        self.assertEqual(len(s._activities), 1)

    def test_svc_level_dupe_tz_uniform(self):
        ''' check that service-level duplicate activities with the same TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = Activity()
        actA.StartTime = pytz.timezone("America/Denver").localize(datetime(1, 2, 3, 4, 5, 6, 7))
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actB = Activity()
        actB.StartTime = actA.StartTime
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)

        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        self.assertEqual(len(s._activities), 1)

    def test_svc_level_dupe_tz_nonuniform(self):
        ''' check that service-level duplicate activities with non-uniform TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actB = Activity()
        actB.StartTime = pytz.timezone("America/Denver").localize(actA.StartTime)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)

        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        self.assertEqual(len(s._activities), 1)

    def test_svc_level_dupe_tz_irregular(self):
        ''' check that service-level duplicate activities with irregular TZs are caught '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = Activity()
        actA.StartTime = pytz.timezone("America/Edmonton").localize(datetime(1, 2, 3, 4, 5, 6, 7))
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actB = Activity()
        actB.StartTime = actA.StartTime.astimezone(pytz.timezone("America/Iqaluit"))
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)

        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        self.assertEqual(len(s._activities), 1)

    def test_svc_level_dupe_time_leeway(self):
        ''' check that service-level duplicate activities within the defined time leeway are caught '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actA.Type = set(svcA.SupportedActivities).intersection(set(svcB.SupportedActivities)).pop()
        actB = Activity()
        actB.StartTime = datetime(1, 2, 3, 4, 6, 6, 7)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)
        actB.Type = actA.Type

        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        self.assertIn(actA.UID, actA.UIDs)
        self.assertIn(actB.UID, actA.UIDs)
        self.assertIn(actA.UID, actB.UIDs)
        self.assertIn(actB.UID, actB.UIDs)

        # we need to fake up the service records to avoid having to call the actual sync method where these values are normally preset
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        recA.SynchronizedActivities = [actA.UID]
        recB.SynchronizedActivities = [actB.UID]

        s._serviceConnections = [recA, recB]
        recipientServicesA = s._determineRecipientServices(actA)
        recipientServicesB = s._determineRecipientServices(actB)

        self.assertEqual(len(recipientServicesA), 0)
        self.assertEqual(len(recipientServicesB), 0)
        self.assertEqual(len(s._activities), 1)

    def test_svc_supported_activity_types(self):
        ''' check that only activities are only sent to services which support them '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        svcA.SupportedActivities = [ActivityType.CrossCountrySkiing]
        svcB.SupportedActivities = [ActivityType.Cycling]

        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actA.Type = svcA.SupportedActivities[0]
        actA.CalculateUID()
        actA.UIDs = set([actA.UID])
        actA.Record = ActivityRecord.FromActivity(actA)

        actB = Activity()
        actB.StartTime = datetime(5, 6, 7, 8, 9, 10, 11)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)
        actB.Type = [x for x in svcB.SupportedActivities if x != actA.Type][0]
        actB.CalculateUID()
        actB.UIDs = set([actB.UID])
        actB.Record = ActivityRecord.FromActivity(actB)

        s = SynchronizationTask(None)
        s._serviceConnections = [recA, recB]
        s._activities = []
        s._accumulateActivities(recA, [actA])
        s._accumulateActivities(recB, [actB])

        syncToA = s._determineRecipientServices(actA)
        syncToB = s._determineRecipientServices(actB)

        self.assertEqual(len(syncToA), 0)
        self.assertEqual(len(syncToB), 0)

        svcB.SupportedActivities = svcA.SupportedActivities

        syncToA = s._determineRecipientServices(actA)
        syncToB = s._determineRecipientServices(actB)

        self.assertEqual(len(syncToA), 1)
        self.assertEqual(len(syncToB), 0)

        svcB.SupportedActivities = svcA.SupportedActivities = [ActivityType.CrossCountrySkiing, ActivityType.Cycling]

        syncToA = s._determineRecipientServices(actA)
        syncToB = s._determineRecipientServices(actB)

        self.assertEqual(len(syncToA), 1)
        self.assertEqual(len(syncToB), 1)

    def test_accumulate_exclusions(self):
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)

        # regular
        s = SynchronizationTask(None)
        s._syncExclusions = {recA._id: {}}
        exc = APIExcludeActivity("Messag!e", activity_id=3.14)
        s._accumulateExclusions(recA, exc)
        exclusionstore = s._syncExclusions
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Message"], "Messag!e")
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Activity"], None)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["ExternalActivityID"], 3.14)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Permanent"], True)

        # updating
        act = TestTools.create_blank_activity(svcA)
        act.UID = "3_14"  # meh
        exc = APIExcludeActivity("Messag!e2", activity_id=42, permanent=False, activity=act)
        s = SynchronizationTask(None)
        s._syncExclusions = {recA._id: {}}
        s._accumulateExclusions(recA, exc)
        exclusionstore = s._syncExclusions
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Message"], "Messag!e2")
        self.assertNotEqual(exclusionstore[recA._id]["3_14"]["Activity"], None)  # Who knows what the string format will be down the road?
        self.assertEqual(exclusionstore[recA._id]["3_14"]["ExternalActivityID"], 42)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Permanent"], False)

        # multiple, retaining existing
        exc2 = APIExcludeActivity("INM", activity_id=13)
        exc3 = APIExcludeActivity("FNIM", activity_id=37)
        s._accumulateExclusions(recA, [exc2, exc3])
        exclusionstore = s._syncExclusions
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertTrue("37" in exclusionstore[recA._id])
        self.assertTrue("13" in exclusionstore[recA._id])

        # don't allow with no identifiers
        exc4 = APIExcludeActivity("nooooo")
        s = SynchronizationTask(None)
        s._syncExclusions = {}
        self.assertRaises(ValueError, s._accumulateExclusions, recA, [exc4])

    def test_activity_deduplicate_normaltz(self):
        ''' ensure that we can't deduplicate activities with non-pytz timezones '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = TestTools.create_random_activity(svcA, tz=UTC())

        actB = Activity()
        actB.StartTime = actA.StartTime.replace(tzinfo=None) + timedelta(seconds=10)
        actB.EndTime = actA.EndTime.replace(tzinfo=None)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)
        actA.Name = "Not this"
        actB.Name = "Heya"
        actB.Type = ActivityType.Walking
        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        self.assertRaises(ValueError, s._accumulateActivities, recA, [copy.deepcopy(actA)])

    def test_activity_deduplicate_tzerror(self):
        ''' Test that probably-duplicate activities with starttimes like 09:12:22 and 15:12:22 (on the same day) are recognized as one '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = TestTools.create_random_activity(svcA, tz=pytz.timezone("America/Iqaluit"))
        actB = Activity()
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=5)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB)
        actA.Name = "Not this"
        actB.Name = "Heya"
        actB.Type = ActivityType.Walking
        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        s._accumulateActivities(recA, [copy.deepcopy(actA)])

        self.assertEqual(len(s._activities), 1)

        # Ensure that it is deduplicated on non-exact match
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=5, seconds=1)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        s._accumulateActivities(recA, [copy.deepcopy(actA)])

        self.assertEqual(len(s._activities), 1)

        # Ensure that it is *not* deduplicated when it really doesn't match
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=5, minutes=7)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        s._accumulateActivities(recA, [copy.deepcopy(actA)])

        self.assertEqual(len(s._activities), 2)

        # Ensure that overly large differences >38hr - not possible via TZ differences & shamefully bad import/export code on the part of some services - are not deduplicated
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=50)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        s._accumulateActivities(recA, [copy.deepcopy(actA)])

        self.assertEqual(len(s._activities), 2)

    def test_activity_coalesce(self):
        ''' ensure that activity data is getting coalesced by _accumulateActivities '''
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        actA = TestTools.create_random_activity(svcA, tz=pytz.timezone("America/Iqaluit"))
        actB = Activity()
        actB.StartTime = actA.StartTime.replace(tzinfo=None)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB)
        actA.Name = "Not this"
        actA.Private = True
        actB.Name = "Heya"
        actB.Type = ActivityType.Walking
        actA.CalculateUID()
        actB.CalculateUID()

        s = SynchronizationTask(None)
        s._activities = []
        s._accumulateActivities(recB, [copy.deepcopy(actB)])
        s._accumulateActivities(recA, [copy.deepcopy(actA)])

        self.assertEqual(len(s._activities), 1)
        act = s._activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertLapsListsEqual(act.Laps, actA.Laps)
        self.assertTrue(act.Private)  # Most restrictive setting
        self.assertEqual(act.Name, actB.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Same here.
        self.assertTrue(list(actB.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)
        self.assertTrue(list(actA.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)

        s._activities = []
        s._accumulateActivities(recA, [copy.deepcopy(actA)])
        s._accumulateActivities(recB, [copy.deepcopy(actB)])

        self.assertEqual(len(s._activities), 1)
        act = s._activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertLapsListsEqual(act.Laps, actA.Laps)
        self.assertEqual(act.Name, actA.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Exception: ActivityType.Other does not take priority
        self.assertTrue(list(actB.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)
        self.assertTrue(list(actA.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)

        # Similar activities should be coalesced (Hiking, Walking..)..
        actA.Type = ActivityType.Hiking
        s._activities = []
        s._accumulateActivities(recA, [copy.deepcopy(actA)])
        s._accumulateActivities(recB, [copy.deepcopy(actB)])

        self.assertEqual(len(s._activities), 1)
        act = s._activities[0]
        self.assertEqual(act.Type, actA.Type)  # Here, it will take priority.

        # Dissimilar should not..
        actA.Type = ActivityType.CrossCountrySkiing
        s._activities = []
        s._accumulateActivities(recA, [copy.deepcopy(actA)])
        s._accumulateActivities(recB, [copy.deepcopy(actB)])

        self.assertEqual(len(s._activities), 2)
        act = s._activities[0]
        self.assertEqual(act.Type, actA.Type)

    def test_eligibility_excluded(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        recipientServices = [recA, recB]
        s = SynchronizationTask(None)
        s._excludedServices = {recA._id: UserException(UserExceptionType.Private)}
        s.user = user
        s._serviceConnections = recipientServices
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
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
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = recipientServices
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recB in eligible)
        self.assertTrue(recA not in eligible)

    def test_eligibility_flowexception(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recA)
        act.Origin = recA
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        User.SetFlowException(user, recA, recB, flowToTarget=False)
        recipientServices = [recA, recB]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = recipientServices
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA in eligible)
        self.assertFalse(recB in eligible)

    def test_eligibility_flowexception_shortcircuit(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        svcC = TestTools.create_mock_service("mockC")
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        recC = TestTools.create_mock_svc_record(svcC)
        act = TestTools.create_blank_activity(svcA, record=recA)
        User.SetFlowException(user, recA, recC, flowToTarget=False)

        # Behaviour with known origin and no override set
        act.Origin = recA
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        recipientServices = [recC, recB]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = [recA, recB, recC]
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB in eligible)
        self.assertTrue(recC not in eligible)

        # Enable alternate routing
        # FIXME: This setting doesn't seem to be used anywhere any more??  Test disabled at the end..
        recB.SetConfiguration({"allow_activity_flow_exception_bypass_via_self": True}, no_save=True)
        self.assertTrue(recB.GetConfiguration()["allow_activity_flow_exception_bypass_via_self"])
        # We should now be able to arrive at recC via recB
        act.Origin = recA
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        recipientServices = [recC, recB]
        s._excludedServices = {}
        s._serviceConnections = [recA, recB, recC]
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB in eligible)
        # self.assertTrue(recC in eligible)

    def test_eligibility_flowexception_reverse(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        User.SetFlowException(user, recA, recB, flowToSource=False)
        recipientServices = [recA, recB]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = recipientServices
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertFalse(recA in eligible)
        self.assertTrue(recB in eligible)

    def test_eligibility_flowexception_both(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        recipientServices = [recA, recB]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = recipientServices
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertFalse(recA in eligible)
        self.assertTrue(recB in eligible)

        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA in eligible)
        self.assertFalse(recB in eligible)

    def test_eligibility_flowexception_none(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)
        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        recipientServices = [recA]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = [recA, recB]
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

    def test_eligibility_flowexception_change(self):
        user = TestTools.create_mock_user()
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        act = TestTools.create_blank_activity(svcA, record=recB)
        act.Origin = recB
        act.UIDs = set([act.UID])
        act.Record = ActivityRecord.FromActivity(act)

        recipientServices = [recA]
        s = SynchronizationTask(None)
        s._excludedServices = {}
        s.user = user
        s._serviceConnections = [recA, recB]

        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=True)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=False)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recA, recB]
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=True)
        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)

        eligible = s._determineEligibleRecipientServices(act, recipientServices)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)
