from tapiriik.testing.testtools import TestTools, TapiriikTestCase
from tapiriik.services.gpx import GPXIO


class GPXTests(TapiriikTestCase):
    def test_constant_representation(self):
        ''' ensures that gpx import/export is symetric '''

        svcA, other = TestTools.create_mock_services()
        svcA.SupportsHR = svcA.SupportsCadence = svcA.SupportsTemp = True
        svcA.SupportsPower = svcA.SupportsCalories = False
        act = TestTools.create_random_activity(svcA, tz=True)

        mid = GPXIO.Dump(act)

        act2 = GPXIO.Parse(mid)
        act2.TZ = act.TZ  # we need to fake this since local TZ isn't defined in GPX files, and TZ discovery will flail with random activities
        act2.AdjustTZ()
        act.Distance = act2.Distance = None  # same here

        self.assertActivitiesEqual(act2, act)
