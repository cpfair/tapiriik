from tapiriik.testing.testtools import TestTools, TapiriikTestCase

from tapiriik.sync import Sync
from tapiriik.services import Service
from tapiriik.services.api import APIExcludeActivity
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

        activities = []



        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

        self.assertEqual(len(activities), 1)

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

        activities = []
        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

        self.assertEqual(len(activities), 1)

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

        activities = []
        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

        self.assertEqual(len(activities), 1)

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

        activities = []
        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

        self.assertEqual(len(activities), 1)

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

        activities = []
        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

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
        recA = TestTools.create_mock_svc_record(svcA)
        recB = TestTools.create_mock_svc_record(svcB)
        svcA.SupportedActivities = [ActivityType.CrossCountrySkiing]
        svcB.SupportedActivities = [ActivityType.Cycling]

        actA = Activity()
        actA.StartTime = datetime(1, 2, 3, 4, 5, 6, 7)
        actA.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        actA.Type = svcA.SupportedActivities[0]
        actB = Activity()
        actB.StartTime = datetime(5, 6, 7, 8, 9, 10, 11)
        actB.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcB, record=recB)
        actB.Type = [x for x in svcB.SupportedActivities if x != actA.Type][0]

        actA.CalculateUID()
        actB.CalculateUID()

        allConns = [recA, recB]

        activities = []
        Sync._accumulateActivities(recA, [actA], activities)
        Sync._accumulateActivities(recB, [actB], activities)

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

    def test_accumulate_exclusions(self):
        svcA, svcB = TestTools.create_mock_services()
        recA = TestTools.create_mock_svc_record(svcA)

        exclusionstore = {recA._id: {}}
        # regular
        exc = APIExcludeActivity("Messag!e", activityId=3.14)
        Sync._accumulateExclusions(recA, exc, exclusionstore)
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Message"], "Messag!e")
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Activity"], None)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["ExternalActivityID"], 3.14)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Permanent"], True)

        # updating
        act = TestTools.create_blank_activity(svcA)
        act.UID = "3_14"  # meh
        exc = APIExcludeActivity("Messag!e2", activityId=42, permanent=False, activity=act)
        Sync._accumulateExclusions(recA, exc, exclusionstore)
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Message"], "Messag!e2")
        self.assertNotEqual(exclusionstore[recA._id]["3_14"]["Activity"], None)  # Who knows what the string format will be down the road?
        self.assertEqual(exclusionstore[recA._id]["3_14"]["ExternalActivityID"], 42)
        self.assertEqual(exclusionstore[recA._id]["3_14"]["Permanent"], False)

        # multiple, retaining existing
        exc2 = APIExcludeActivity("INM", activityId=13)
        exc3 = APIExcludeActivity("FNIM", activityId=37)
        Sync._accumulateExclusions(recA, [exc2, exc3], exclusionstore)
        self.assertTrue("3_14" in exclusionstore[recA._id])
        self.assertTrue("37" in exclusionstore[recA._id])
        self.assertTrue("13" in exclusionstore[recA._id])

        # don't allow with no identifiers
        exc4 = APIExcludeActivity("nooooo")
        self.assertRaises(ValueError, Sync._accumulateExclusions, recA, [exc4], exclusionstore)

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

        activities = []
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)
        self.assertRaises(ValueError, Sync._accumulateActivities, recA, [copy.deepcopy(actA)], activities)

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

        activities = []
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)

        self.assertEqual(len(activities), 1)

        # Ensure that it is an exact match
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=5, seconds=1)
        activities = []
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)

        self.assertEqual(len(activities), 2)

        # Ensure that overly large differences >38hr - not possible via TZ differences & shamefully bad import/export code on the part of some services - are not deduplicated
        actB.StartTime = actA.StartTime.replace(tzinfo=pytz.timezone("America/Denver")) + timedelta(hours=50)
        activities = []
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)

        self.assertEqual(len(activities), 2)

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

        activities = []
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)

        self.assertEqual(len(activities), 1)
        act = activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.Waypoints, actA.Waypoints)
        self.assertTrue(act.Private)  # Most restrictive setting
        self.assertEqual(act.Name, actB.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Same here.
        self.assertTrue(list(actB.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)
        self.assertTrue(list(actA.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)

        activities = []
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)

        self.assertEqual(len(activities), 1)
        act = activities[0]

        self.assertEqual(act.StartTime, actA.StartTime)
        self.assertEqual(act.EndTime, actA.EndTime)
        self.assertEqual(act.EndTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.StartTime.tzinfo, actA.StartTime.tzinfo)
        self.assertEqual(act.Waypoints, actA.Waypoints)
        self.assertEqual(act.Name, actA.Name)  # The first activity takes priority.
        self.assertEqual(act.Type, actB.Type)  # Exception: ActivityType.Other does not take priority
        self.assertTrue(list(actB.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)
        self.assertTrue(list(actA.ServiceDataCollection.keys())[0] in act.ServiceDataCollection)

        actA.Type = ActivityType.CrossCountrySkiing
        activities = []
        Sync._accumulateActivities(recA, [copy.deepcopy(actA)], activities)
        Sync._accumulateActivities(recB, [copy.deepcopy(actB)], activities)

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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        recipientServices = [recC, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB, recC], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB in eligible)
        self.assertTrue(recC not in eligible)

        # Enable alternate routing
        recB.SetConfiguration({"allow_activity_flow_exception_bypass_via_self":True}, no_save=True)
        self.assertTrue(recB.GetConfiguration()["allow_activity_flow_exception_bypass_via_self"])
        # We should now be able to arrive at recC via recB
        act.Origin = recA
        recipientServices = [recC, recB]
        excludedServices = []
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB, recC], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB in eligible)
        self.assertTrue(recC in eligible)

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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertFalse(recA in eligible)
        self.assertTrue(recB in eligible)

        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=recipientServices, recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
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
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recB]
        act.Origin = recA
        act.ServiceDataCollection = TestTools.create_mock_servicedatacollection(svcA, record=recA)
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=False)
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        User.SetFlowException(user, recA, recB, flowToSource=False, flowToTarget=False)
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA not in eligible)
        self.assertTrue(recB not in eligible)

        recipientServices = [recA, recB]
        User.SetFlowException(user, recA, recB, flowToSource=True, flowToTarget=True)
        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)

        eligible = Sync._determineEligibleRecipientServices(activity=act, connectedServices=[recA, recB], recipientServices=recipientServices, excludedServices=excludedServices, user=user)
        self.assertTrue(recA in eligible)
        self.assertTrue(recB in eligible)