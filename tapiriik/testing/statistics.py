from tapiriik.testing.testtools import TapiriikTestCase

from tapiriik.services.interchange import ActivityStatistic, ActivityStatisticUnit


class StatisticTests(TapiriikTestCase):

    def test_unitconv_temp(self):
        stat = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, value=0)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.DegreesFahrenheit).Value, 32)

        stat = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, value=-40)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.DegreesFahrenheit).Value, -40)

        stat = ActivityStatistic(ActivityStatisticUnit.DegreesFahrenheit, value=-40)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.DegreesCelcius).Value, -40)

        stat = ActivityStatistic(ActivityStatisticUnit.DegreesFahrenheit, value=32)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.DegreesCelcius).Value, 0)

    def test_unitconv_distance_nonmetric(self):
        stat = ActivityStatistic(ActivityStatisticUnit.Miles, value=1)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.Feet).Value, 5280)

        stat = ActivityStatistic(ActivityStatisticUnit.Feet, value=5280/2)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.Miles).Value, 0.5)

    def test_unitconv_distance_metric(self):
        stat = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=1)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.Meters).Value, 1000)

        stat = ActivityStatistic(ActivityStatisticUnit.Meters, value=250)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.Kilometers).Value, 0.25)

    def test_unitconv_distance_cross(self):
        stat = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=1)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.Miles).Value, 0.6214, places=4)

        stat = ActivityStatistic(ActivityStatisticUnit.Miles, value=1)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.Kilometers).Value, 1.609, places=3)

        stat = ActivityStatistic(ActivityStatisticUnit.Miles, value=1)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.Meters).Value, 1609, places=0)

    def test_unitconv_velocity_metric(self):
        stat = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, value=100)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.KilometersPerHour).Value, 360)

        stat = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, value=50)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.MetersPerSecond).Value, 13.89, places=2)

    def test_unitconv_velocity_cross(self):
        stat = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, value=100)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.MilesPerHour).Value, 62, places=0)

        stat = ActivityStatistic(ActivityStatisticUnit.MilesPerHour, value=60)
        self.assertAlmostEqual(stat.asUnits(ActivityStatisticUnit.KilometersPerHour).Value, 96.5, places=0)

    def test_unitconv_impossible(self):
        stat = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, value=100)
        self.assertRaises(ValueError, stat.asUnits, ActivityStatisticUnit.Meters)

        stat = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, value=100)
        self.assertRaises(ValueError, stat.asUnits, ActivityStatisticUnit.Miles)

    def test_unitconv_noop(self):
        stat = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, value=100)
        self.assertEqual(stat.asUnits(ActivityStatisticUnit.KilometersPerHour).Value, 100)

    def test_stat_coalesce(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat1.coalesceWith(stat2)
        self.assertEqual(stat1.Value, 1.5)

    def test_stat_coalesce_missing(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat1.coalesceWith(stat2)
        self.assertEqual(stat1.Value, 2)

        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None)
        stat1.coalesceWith(stat2)
        self.assertEqual(stat1.Value, 1)

    def test_stat_coalesce_multi(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5)
        stat1.coalesceWith(stat2)
        stat1.coalesceWith(stat3)
        stat1.coalesceWith(stat4)
        stat1.coalesceWith(stat5)
        self.assertEqual(stat1.Value, 3)

    def test_stat_coalesce_multi_mixed(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5)
        stat5.coalesceWith(stat2)
        stat5.coalesceWith(stat3)
        stat1.coalesceWith(stat5)
        stat1.coalesceWith(stat4)

        self.assertEqual(stat1.Value, 3)

    def test_stat_coalesce_multi_mixed2(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5)
        stat5.coalesceWith(stat2)
        stat3.coalesceWith(stat5)
        stat4.coalesceWith(stat3)
        stat1.coalesceWith(stat4)

        self.assertEqual(stat1.Value, 3)

    def test_stat_coalesce_multi_missingmixed(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=1)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5)
        stat5.coalesceWith(stat2)
        stat3.coalesceWith(stat5)
        stat4.coalesceWith(stat3)
        stat1.coalesceWith(stat4)

        self.assertAlmostEqual(stat1.Value, 8/3)

    def test_stat_coalesce_multi_missingmixed_multivalued(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, min=None)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2, max=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, gain=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, loss=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5, min=3)
        stat5.coalesceWith(stat2)
        stat3.coalesceWith(stat5)
        stat4.coalesceWith(stat3)
        stat1.coalesceWith(stat4)

        self.assertAlmostEqual(stat1.Value, 7/2)
        self.assertEqual(stat1.Min, 3)
        self.assertEqual(stat1.Max, 2)
        self.assertEqual(stat1.Gain, 3)
        self.assertEqual(stat1.Loss, 4)

    def test_stat_sum(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, min=None)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2, max=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, gain=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, gain=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5, max=3)
        stat5.sumWith(stat2)
        stat3.sumWith(stat5)
        stat4.sumWith(stat3)
        stat1.sumWith(stat4)

        self.assertEqual(stat1.Value, 7)
        self.assertEqual(stat1.Max, 3)
        self.assertEqual(stat1.Gain, 7)

    def test_stat_update(self):
        stat1 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, min=None)
        stat2 = ActivityStatistic(ActivityStatisticUnit.Meters, value=2, max=2)
        stat3 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, gain=3)
        stat4 = ActivityStatistic(ActivityStatisticUnit.Meters, value=None, gain=4)
        stat5 = ActivityStatistic(ActivityStatisticUnit.Meters, value=5, max=3)
        stat5.update(stat2)
        stat3.update(stat5)
        stat4.update(stat3)
        stat1.update(stat4)

        self.assertEqual(stat1.Value, 2)
        self.assertEqual(stat1.Max, 2)
        self.assertEqual(stat1.Gain, 3)
