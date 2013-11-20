from datetime import timedelta, datetime
from tapiriik.database import cachedb
from tapiriik.database.tz import TZLookup
import hashlib
import pytz


class ActivityType:  # taken from RK API docs. The text values have no meaning except for debugging
    Running = "Running"
    Cycling = "Cycling"
    MountainBiking = "MtnBiking"
    Walking = "Walking"
    Hiking = "Hiking"
    DownhillSkiing = "DownhillSkiing"
    CrossCountrySkiing = "XCSkiing"
    Snowboarding = "Snowboarding"
    Skating = "Skating"
    Swimming = "Swimming"
    Wheelchair = "Wheelchair"
    Rowing = "Rowing"
    Elliptical = "Elliptical"
    Other = "Other"

    # The right-most element is the "most specific."
    _hierarchy = [
        [Cycling, MountainBiking],
        [Running, Walking, Hiking]
    ]
    def PickMostSpecific(types):
        types = [x for x in types if x and x is not ActivityType.Other]
        if len(types) == 0:
            return ActivityType.Other
        most_specific = types[0]
        for definition in ActivityType._hierarchy:
            if len([x for x in types if x in definition]) == len(types):
                for act_type in types:
                    if definition.index(most_specific) < definition.index(act_type):
                        most_specific = act_type
        return most_specific


class Activity:
    def __init__(self, startTime=None, endTime=None, actType=ActivityType.Other, distance=None, name=None, notes=None, tz=None, lapList=None, private=False, fallbackTz=None, stationary=False):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Type = actType
        self.Laps = lapList if lapList is not None else []
        self.Stats = ActivityStatistics(distance=distance)
        self.TZ = tz
        self.FallbackTZ = fallbackTz
        self.Name = name
        self.Notes = notes
        self.Private = private
        self.Stationary = stationary
        self.PrerenderedFormats = {}

    def CalculateUID(self):
        if not self.StartTime:
            return  # don't even try
        csp = hashlib.new("md5")
        roundedStartTime = self.StartTime
        roundedStartTime = roundedStartTime - timedelta(microseconds=roundedStartTime.microsecond)
        if self.TZ:
            roundedStartTime = roundedStartTime.astimezone(self.TZ)
        csp.update(roundedStartTime.strftime("%Y-%m-%d %H:%M:%S").encode('utf-8'))  # exclude TZ for compat
        self.UID = csp.hexdigest()

    def CountTotalWaypoints(self):
        return sum([len(x.Waypoints) for x in self.Laps])

    def GetFirstWaypointWithLocation(self):
        loc_wp = None
        for lap in self.Laps:
            for wp in lap.Waypoints:
                if wp.Location is not None and wp.Location.Latitude is not None and wp.Location.Longitude is not None:
                    loc_wp = wp.Location
                    break
        return loc_wp

    def DefineTZ(self):
        """ run localize() on all contained dates (doesn't change values) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        if self.StartTime and self.StartTime.tzinfo is None:
            self.StartTime = self.TZ.localize(self.StartTime)
        if self.EndTime and self.EndTime.tzinfo is None:
            self.EndTime = self.TZ.localize(self.EndTime)
        for lap in self.Laps:
            lap.StartTime = self.TZ.localize(lap.StartTime) if lap.StartTime.tzinfo is None else lap.StartTime
            lap.EndTime = self.TZ.localize(lap.EndTime) if lap.EndTime.tzinfo is None else lap.EndTime
            for wp in lap.Waypoints:
                if wp.Timestamp.tzinfo is None:
                    wp.Timestamp = self.TZ.localize(wp.Timestamp)
        self.CalculateUID()

    def AdjustTZ(self):
        """ run astimezone() on all contained dates (requires non-naive DTs) """
        if self.TZ is None:
            raise ValueError("TZ not set")
        self.StartTime = self.StartTime.astimezone(self.TZ)
        self.EndTime = self.EndTime.astimezone(self.TZ)

        for lap in self.Laps:
            lap.StartTime = lap.StartTime.astimezone(self.TZ)
            lap.EndTime = lap.EndTime.astimezone(self.TZ)
            for wp in lap.Waypoints:
                    wp.Timestamp = wp.Timestamp.astimezone(self.TZ)
        self.CalculateUID()

    def CalculateTZ(self, loc=None, recalculate=False):
        if self.TZ and not recalculate:
            return self.TZ
        if loc is None:
            loc = self.GetFirstWaypointWithLocation()
            if loc is None and self.FallbackTZ is None:
                raise Exception("Can't find TZ without a waypoint with a location, or a fallback TZ")
        if loc is None:
            # At this point, we'll resort to the fallback TZ.
            if self.FallbackTZ is None:
                raise Exception("Can't find TZ without a waypoint with a location, specified location, or fallback TZ")
            self.TZ = self.FallbackTZ
            return self.TZ
        # I guess at some point it will be faster to perform a full lookup than digging through this table.
        cachedTzData = cachedb.tz_cache.find_one({"Latitude": loc.Latitude, "Longitude": loc.Longitude})
        if cachedTzData is None:
            res = TZLookup(loc.Latitude, loc.Longitude)
            cachedTzData = {"TZ": res, "Latitude": loc.Latitude, "Longitude": loc.Longitude}
            cachedb.tz_cache.insert(cachedTzData)

        if type(cachedTzData["TZ"]) != str:
            self.TZ = pytz.FixedOffset(cachedTzData["TZ"] * 60)
        else:
            self.TZ = pytz.timezone(cachedTzData["TZ"])
        return self.TZ

    def EnsureTZ(self, recalculate=False):
        self.CalculateTZ(recalculate=recalculate)
        if self.StartTime.tzinfo is None:
            self.DefineTZ()
        else:
            self.AdjustTZ()

    def CheckSanity(self):
        """ Started out as a function that checked to make sure the activity itself is sane.
            Now we perform a lot of checks to make sure all the objects were initialized properly
            I'm undecided on this front...
                - Forcing the .NET model of "XYZCollection"s that enforce integrity seems wrong
                - Enforcing them in constructors makes using the classes a pain
        """
        if not hasattr(self, "ServiceDataCollection") or len(self.ServiceDataCollection.keys()) == 0:
            raise ValueError("Unset ServiceData/ServiceDataCollection field")
        srcs = self.ServiceDataCollection  # this is just so I can see the source of the activity in the exception message
        if self.TZ and self.TZ.utcoffset(self.StartTime.replace(tzinfo=None)) != self.StartTime.tzinfo.utcoffset(self.StartTime.replace(tzinfo=None)):
            raise ValueError("Inconsistent timezone between StartTime (" + str(self.StartTime) + ") and activity (" + str(self.TZ) + ")")
        if self.TZ and self.TZ.utcoffset(self.EndTime.replace(tzinfo=None)) != self.StartTime.tzinfo.utcoffset(self.EndTime.replace(tzinfo=None)):
            raise ValueError("Inconsistent timezone between EndTime (" + str(self.EndTime) + ") and activity (" + str(self.TZ) + ")")
        if len(self.Laps) == 0:
                raise ValueError("No laps")
        wptCt = sum([len(x.Waypoints) for x in self.Laps])
        if not self.Stationary:
            if wptCt == 0:
                raise ValueError("Exactly 0 waypoints")
            if wptCt == 1:
                raise ValueError("Only 1 waypoint")
        if self.Stats.Distance.Value is not None and self.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value > 1000 * 1000:
            raise ValueError("Exceedingly long activity (distance)")
        if self.StartTime.replace(tzinfo=None) > (datetime.now() + timedelta(days=5)):
            raise ValueError("Activity is from the future")
        if self.EndTime and self.EndTime.replace(tzinfo=None) > (datetime.now() + timedelta(days=5 + 5)): # Based on the 5-day activity length limit imposed later.
            raise ValueError("Activity ends in the future")

        if self.StartTime and self.EndTime:
            # We can only do these checks if the activity has both start and end times (Dropbox)
            if (self.EndTime - self.StartTime).total_seconds() < 0:
                raise ValueError("Event finishes before it starts")
            if (self.EndTime - self.StartTime).total_seconds() == 0:
                raise ValueError("0-duration activity")
            if (self.EndTime - self.StartTime).total_seconds() > 60 * 60 * 24 * 5:
                raise ValueError("Exceedingly long activity (time)")

        if len(self.Laps) == 1:
            if self.Laps[0].Stats != self.Stats:
                raise ValueError("Activity with 1 lap has mismatching statistics between activity and lap")
        if len(self.Laps) >= 1:
            if self.Laps[0].StartTime != self.StartTime:
                raise ValueError("Start time of first lap must match start time of activity")
            if self.Laps[-1].EndTime != self.EndTime:
                raise ValueError("End time of last lap must match end time of activity")
        altLow = None
        altHigh = None
        pointsWithoutLocation = 0
        for lap in self.Laps:
            if not lap.StartTime:
                raise ValueError("Lap has no start time")
            if not lap.EndTime:
                raise ValueError("Lap has no start time")
            if len(lap.Waypoints):
                if lap.Waypoints[0].Timestamp != lap.StartTime:
                    raise ValueError("Lap start time does not match first waypoint timestamp")
                if lap.Waypoints[-1].Timestamp != lap.EndTime:
                    raise ValueError("Lap end time does not match last waypoint timestamp")
            for wp in lap.Waypoints:
                if self.TZ and self.TZ.utcoffset(wp.Timestamp.replace(tzinfo=None)) != wp.Timestamp.tzinfo.utcoffset(wp.Timestamp.replace(tzinfo=None)):
                    raise ValueError("WP " + str(wp.Timestamp) + " and activity timezone (" + str(self.TZ) + ") are inconsistent")
                if wp.Location:
                    if wp.Location.Latitude == 0 and wp.Location.Longitude == 0:
                        raise ValueError("Invalid lat/lng")
                    if (wp.Location.Latitude is not None and (wp.Location.Latitude > 90 or wp.Location.Latitude < -90)) or (wp.Location.Longitude is not None and (wp.Location.Longitude > 180 or wp.Location.Longitude < -180)):
                        raise ValueError("Out of range lat/lng")
                    if wp.Location.Altitude is not None and (altLow is None or wp.Location.Altitude < altLow):
                        altLow = wp.Location.Altitude
                    if wp.Location.Altitude is not None and (altHigh is None or wp.Location.Altitude > altHigh):
                        altHigh = wp.Location.Altitude
                if not wp.Location or wp.Location.Latitude is None or wp.Location.Longitude is None:
                    pointsWithoutLocation += 1
        if wptCt - pointsWithoutLocation == 0 and not self.Stationary:
            raise ValueError("No points have location")
        if wptCt - pointsWithoutLocation == 1:
            raise ValueError("Only one point has location")
        if altLow is not None and altLow == altHigh and altLow == 0:  # some activities have very sporadic altitude data, we'll let it be...
            raise ValueError("Invalid altitudes / no change from " + str(altLow))

    def __str__(self):
        return "Activity (" + self.Type + ") Start " + str(self.StartTime) + " " + str(self.StartTime.tzinfo if self.StartTime else "") + " End " + str(self.EndTime) + " " + str(len(self.Laps)) + " laps " + str(sum([len(x.Waypoints) for x in self.Laps])) + " WPs"
    __repr__ = __str__

    def __eq__(self, other):
        # might need to fix this for TZs?
        return self.StartTime == other.StartTime and self.EndTime == other.EndTime and self.Type == other.Type and self.Laps == other.Laps and self.Stats.Distance == other.Stats.Distance and self.Name == other.Name

    def __ne__(self, other):
        return not self.__eq__(other)


class UploadedActivity (Activity):
    pass  # will contain list of which service instances contain this activity - not really merited

class LapIntensity:
    Active = 0
    Rest = 1
    Warmup = 2
    Cooldown = 3

class LapTriggerMethod:
    Manual = 0
    Time = 1
    Distance = 2
    PositionStart = 3
    PositionLap = 4
    PositionWaypoint = 5
    PositionMarked = 6
    SessionEnd = 7
    FitnessEquipment = 8

class Lap:
    def __init__(self, startTime=None, endTime=None, intensity=LapIntensity.Active, trigger=LapTriggerMethod.Manual, stats=None, waypointList=None):
        self.StartTime = startTime
        self.EndTime = endTime
        self.Trigger = trigger
        self.Intensity = intensity
        self.Stats = stats if stats else ActivityStatistics()
        self.Waypoints = waypointList if waypointList else []

class ActivityStatistics:
    _statKeyList = ["Distance", "MovingTime", "Kilocalories", "Speed", "Elevation", "HR", "Cadence", "RunCadence", "Strides", "Temperature", "Power"]
    def __init__(self, distance=None, moving_time=None, avg_speed=None, max_speed=None, max_elevation=None, min_elevation=None, gained_elevation=None, lost_elevation=None, avg_hr=None, max_hr=None, avg_cadence=None, max_cadence=None, avg_run_cadence=None, max_run_cadence=None, strides=None, min_temp=None, avg_temp=None, max_temp=None, kcal=None, avg_power=None, max_power=None):
        self.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=distance)
        self.MovingTime = ActivityStatistic(ActivityStatisticUnit.Time, value=moving_time)  # timedelta()
        self.Kilocalories = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=kcal) # KCal
        self.Speed = ActivityStatistic(ActivityStatisticUnit.KilometersPerHour, avg=avg_speed, max=max_speed)
        self.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, max=max_elevation, min=min_elevation, gain=gained_elevation, loss=lost_elevation)
        self.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=avg_hr, max=max_hr)
        self.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=avg_cadence, max=max_cadence)
        self.RunCadence = ActivityStatistic(ActivityStatisticUnit.StepsPerMinute, avg=avg_run_cadence, max=max_run_cadence)
        self.Strides = ActivityStatistic(ActivityStatisticUnit.Strides, value=strides)
        self.Temperature = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, avg=avg_temp, max=max_temp, min=min_temp)
        self.Power = ActivityStatistic(ActivityStatisticUnit.Watts, avg=avg_power, max=max_power)

    def coalesceWith(self, other_stats):
        for stat in ActivityStatistics._statKeyList:
            self.__dict__[stat].coalesceWith(other_stats.__dict__[stat])
    # Magic dict is meh
    def update(self, other_stats):
        for stat in ActivityStatistics._statKeyList:
            self.__dict__[stat].update(other_stats.__dict__[stat])

    def __eq__(self, other):
        if not other:
            return False
        for stat in ActivityStatistics._statKeyList:
            if not self.__dict__[stat] == other_stats.__dict__[stat]:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

class ActivityStatistic:
    def __init__(self, units, value=None, avg=None, min=None, max=None, gain=None, loss=None):
        self.Value = value
        self.Average = avg
        self.Min = min
        self.Max = max
        self.Gain = gain
        self.Loss = loss

        self.Samples = {}
        self.Samples["Value"] = 1 if value is not None else 0
        self.Samples["Average"] = 1 if avg is not None else 0
        self.Samples["Min"] = 1 if min is not None else 0
        self.Samples["Max"] = 1 if max is not None else 0
        self.Samples["Gain"] = 1 if gain is not None else 0
        self.Samples["Loss"] = 1 if loss is not None else 0

        self.Units = units

    def asUnits(self, units):
        if units == self.Units:
            return self
        newStat = ActivityStatistic(units)
        existing_dict = dict(self.__dict__)
        del existing_dict["Units"]
        del existing_dict["Samples"]
        ActivityStatistic.convertUnitsInDict(existing_dict, self.Units, units)
        newStat.__dict__ = existing_dict
        newStat.Units = units
        newStat.Samples = self.Samples
        return newStat

    def convertUnitsInDict(values_dict, from_units, to_units):
        for key, value in values_dict.items():
            if value is None:
                continue
            values_dict[key] = ActivityStatistic.convertValue(value, from_units, to_units)

    def convertValue(value, from_units, to_units):
        conversions = {
            (ActivityStatisticUnit.KilometersPerHour, ActivityStatisticUnit.MilesPerHour): 0.621371,
            (ActivityStatisticUnit.MetersPerSecond, ActivityStatisticUnit.KilometersPerHour): 3.6,
            (ActivityStatisticUnit.DegreesCelcius, ActivityStatisticUnit.DegreesFahrenheit): (lambda C: C*9/5 + 32, lambda F: (F-32) * 5/9),
            (ActivityStatisticUnit.Kilometers, ActivityStatisticUnit.Meters): 1000,
            (ActivityStatisticUnit.Meters, ActivityStatisticUnit.Feet): 3.281,
            (ActivityStatisticUnit.Miles, ActivityStatisticUnit.Feet): 5280
        }
        def recurseFindConversionPath(unit, target, stack):
            assert(unit != target)
            for transform in conversions.keys():
                if unit in transform:
                    if transform in stack:
                        continue  # Prevent circular conversion
                    if target in transform:
                        # We've arrived at the end
                        return stack + [transform]
                    else:
                        next_unit = transform[0] if transform[1] == unit else transform[1]
                        result = recurseFindConversionPath(next_unit, target, stack + [transform])
                        if result:
                            return result
            return None

        conversionPath = recurseFindConversionPath(from_units, to_units, [])
        if not conversionPath:
            raise ValueError("No conversion from %s to %s" % (from_units, to_units))
        for transform in conversionPath:
            if type(conversions[transform]) is float or type(conversions[transform]) is int:
                if from_units == transform[0]:
                    value = value * conversions[transform]
                    from_units = transform[1]
                else:
                    value = value / conversions[transform]
                    from_units = transform[0]
            else:
                if from_units == transform[0]:
                    func = conversions[transform][0] if type(conversions[transform]) is tuple else conversions[transform]
                    value = func(value)
                    from_units = transform[1]
                else:
                    if type(conversions[transform]) is not tuple:
                        raise ValueError("No transform function for %s to %s" % (from_units, to_units))
                    value = conversions[transform][1](value)
                    from_units = transform[0]
        return value

    def coalesceWith(self, stat):
        stat = stat.asUnits(self.Units)

        items = ["Value", "Max", "Min", "Average", "Gain", "Loss"]
        my_items = self.__dict__
        other_items = stat.__dict__
        my_samples = self.Samples
        other_samples = stat.Samples
        for item in items:
            # Only average if there's a second value
            if other_items[item] is not None:
                if my_items[item] is None:
                    # We don't have this item's value, nothing to do really.
                    my_items[item] = other_items[item]
                    my_samples[item] = other_samples[item]
                else:
                    print("Coalesce %s %s n=%s with %s n=%s" % (item, my_items[item], my_samples[item], other_items[item], other_samples[item]))
                    my_items[item] += (other_items[item] - my_items[item]) / ((my_samples[item] + 1 / other_samples[item]))
                    my_samples[item] += other_samples[item]

    def update(self, stat):
        stat = stat.asUnits(self.Units)
        items = ["Value", "Max", "Min", "Average", "Gain", "Loss"]
        other_items = stat.__dict__
        for item in items:
            if item in other_items and other_items[item] is not None:
                self.__dict__[item] = other_items[item]
                self.Samples[item] = stat.Samples[item]

    def __eq__(self, other):
        if not other:
            return False
        return self.Units == other.Units and self.Value == other.Value and self.Average == other.Average and self.Max == other.Max and self.Min == other.Min and self.Gain == other.Gain and self.Loss == other.Loss

    def __ne__(self, other):
        return not self.__eq__(other)



class ActivityStatisticUnit:
    Time = "s"
    Meters = "m"
    Kilometers = "km"
    Feet = "f"
    Miles = "mi"
    DegreesCelcius = "ºC"
    DegreesFahrenheit = "ºF"
    KilometersPerHour = "km/h"
    MetersPerSecond = "m/s"
    MilesPerHour = "mph"
    BeatsPerMinute = "BPM"
    RevolutionsPerMinute = "RPM"
    StepsPerMinute = "SPM"
    Strides = "strides"
    Kilocalories = "kcal"
    Watts = "W"


class WaypointType:
    Start = 0   # Start of activity
    Regular = 1 # Normal
    Pause = 11  # All waypoints within a paused period should have this type
    Resume = 12 # The first waypoint after a paused period
    End = 100   # End of activity

class Waypoint:
    __slots__ = ["Timestamp", "Location", "HR", "Calories", "Power", "Temp", "Cadence", "RunCadence", "Type", "Distance", "Speed"]
    def __init__(self, timestamp=None, ptType=WaypointType.Regular, location=None, hr=None, power=None, calories=None, cadence=None, runCadence=None, temp=None, distance=None, speed=None):
        self.Timestamp = timestamp
        self.Location = location
        self.HR = hr
        self.Calories = calories
        self.Power = power  # I doubt there will ever be more parameters than this in terms of interchange
        self.Temp = temp  # never say never
        self.Cadence = cadence  # dammit this better be the last one
        self.RunCadence = runCadence  # screw it
        self.Distance = distance # I don't even care any more.
        self.Speed = speed # neghhhhh
        self.Type = ptType

    def __eq__(self, other):
        return self.Timestamp == other.Timestamp and self.Location == other.Location and self.HR == other.HR and self.Calories == other.Calories and self.Temp == other.Temp and self.Cadence == other.Cadence and self.Type == other.Type and self.Power == other.Power and self.RunCadence == other.RunCadence and self.Distance == other.Distance and self.Speed == other.Speed

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        if self.Location is None:
            return str(self.Type)+"@"+str(self.Timestamp)
        return str(self.Type) + "@" + str(self.Timestamp) + " " + str(self.Location.Latitude) + "|" + str(self.Location.Longitude) + "^" + str(round(self.Location.Altitude) if self.Location.Altitude is not None else None) + "\n\tHR " + str(self.HR) + " CAD " + str(self.Cadence) + " TEMP " + str(self.Temp) + " PWR " + str(self.Power) + " CAL " + str(self.Calories)
    __repr__ = __str__


class Location:
    __slots__ = ["Latitude", "Longitude", "Altitude"]
    def __init__(self, lat, lon, alt):
        self.Latitude = lat
        self.Longitude = lon
        self.Altitude = alt

    def __eq__(self, other):
        if not other:
            return False
        return self.Latitude == other.Latitude and self.Longitude == other.Longitude and self.Altitude == other.Altitude

    def __ne__(self, other):
        return not self.__eq__(other)
