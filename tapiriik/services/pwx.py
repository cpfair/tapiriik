from lxml import etree
import copy
import dateutil.parser
from datetime import timedelta
from .interchange import WaypointType, ActivityType, Activity, Waypoint, Location, Lap, ActivityStatistic, ActivityStatisticUnit

class PWXIO:
    Namespaces = {
        None: "http://www.peaksware.com/PWX/1/0"
    }

    _sportTypeMappings = {
        "Bike": ActivityType.Cycling,
        "Run": ActivityType.Running,
        "Walk": ActivityType.Walking,
        "Swim": ActivityType.Swimming,
        "Mountain Bike": ActivityType.MountainBiking,
        "XC Ski": ActivityType.CrossCountrySkiing,
        "Rowing": ActivityType.Rowing,
        "Other": ActivityType.Other
    }

    _reverseSportTypeMappings = {
        ActivityType.Cycling: "Bike",
        ActivityType.Running: "Run",
        ActivityType.Walking: "Walk",
        ActivityType.Hiking: "Walk", # Hilly walking?
        ActivityType.Swimming: "Swim",
        ActivityType.MountainBiking: "Mountain Bike",
        ActivityType.CrossCountrySkiing: "XC Ski",
        ActivityType.DownhillSkiing: "XC Ski", # For whatever reason there's no "ski" type
        ActivityType.Rowing: "Rowing",
        ActivityType.Other: "Other",
    }

    def Parse(pwxData, activity=None):
        ns = copy.deepcopy(PWXIO.Namespaces)
        ns["pwx"] = ns[None]
        del ns[None]

        activity = activity if activity else Activity()

        try:
            root = etree.XML(pwxData)
        except:
            root = etree.fromstring(pwxData)

        xworkout = root.find("pwx:workout", namespaces=ns)

        xsportType = xworkout.find("pwx:sportType", namespaces=ns)
        if xsportType is not None:
            sportType = xsportType.text
            if sportType in PWXIO._sportTypeMappings:
                if PWXIO._sportTypeMappings[sportType] != ActivityType.Other:
                    activity.Type = PWXIO._sportTypeMappings[sportType]

        xtitle = xworkout.find("pwx:title", namespaces=ns)
        if xtitle is not None:
            activity.Name = xtitle.text

        xcmt = xworkout.find("pwx:cmt", namespaces=ns)
        if xcmt is not None:
            activity.Notes = xcmt.text

        xtime = xworkout.find("pwx:time", namespaces=ns)
        if xtime is None:
            raise ValueError("Can't parse PWX without time")

        activity.StartTime = dateutil.parser.parse(xtime.text)
        activity.GPS = False

        def _minMaxAvg(xminMaxAvg):
            return {"min": float(xminMaxAvg.attrib["min"]) if "min" in xminMaxAvg.attrib else None, "max": float(xminMaxAvg.attrib["max"]) if "max" in xminMaxAvg.attrib else None, "avg": float(xminMaxAvg.attrib["avg"])  if "avg" in xminMaxAvg.attrib else None} # Most useful line ever

        def _readSummaryData(xsummary, obj, time_ref):
            obj.StartTime = time_ref + timedelta(seconds=float(xsummary.find("pwx:beginning", namespaces=ns).text))
            obj.EndTime = obj.StartTime + timedelta(seconds=float(xsummary.find("pwx:duration", namespaces=ns).text))

            # "duration - durationstopped = moving time. duration stopped may be zero." - Ben
            stoppedEl = xsummary.find("pwx:durationstopped", namespaces=ns)
            if stoppedEl is not None:
                obj.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=(obj.EndTime - obj.StartTime).total_seconds() - float(stoppedEl.text))
            else:
                obj.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=(obj.EndTime - obj.StartTime).total_seconds())

            hrEl = xsummary.find("pwx:hr", namespaces=ns)
            if hrEl is not None:
                obj.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, **_minMaxAvg(hrEl))

            spdEl = xsummary.find("pwx:spd", namespaces=ns)
            if spdEl is not None:
                obj.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, **_minMaxAvg(spdEl))

            pwrEl = xsummary.find("pwx:pwr", namespaces=ns)
            if pwrEl is not None:
                obj.Stats.Power = ActivityStatistic(ActivityStatisticUnit.Watts, **_minMaxAvg(pwrEl))

            cadEl = xsummary.find("pwx:cad", namespaces=ns)
            if cadEl is not None:
                obj.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, **_minMaxAvg(cadEl))

            distEl = xsummary.find("pwx:dist", namespaces=ns)
            if distEl is not None:
                obj.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=float(distEl.text))

            altEl = xsummary.find("pwx:alt", namespaces=ns)
            if altEl is not None:
                obj.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, **_minMaxAvg(altEl))

            climbEl = xsummary.find("pwx:climbingelevation", namespaces=ns)
            if climbEl is not None:
                obj.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, gain=float(climbEl.text)))

            descEl = xsummary.find("pwx:descendingelevation", namespaces=ns)
            if descEl is not None:
                obj.Stats.Elevation.update(ActivityStatistic(ActivityStatisticUnit.Meters, loss=float(descEl.text)))

            tempEl = xsummary.find("pwx:temp", namespaces=ns)
            if tempEl is not None:
                obj.Stats.Temperature = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius, **_minMaxAvg(tempEl))

        _readSummaryData(xworkout.find("pwx:summarydata", namespaces=ns), activity, time_ref=activity.StartTime)

        laps = []
        xsegments = xworkout.findall("pwx:segment", namespaces=ns)

        for xsegment in xsegments:
            lap = Lap()
            _readSummaryData(xsegment.find("pwx:summarydata", namespaces=ns), lap, time_ref=activity.StartTime)
            laps.append(lap)

        if len(laps) == 1:
            laps[0].Stats.update(activity.Stats)
            activity.Stats = laps[0].Stats
        elif not len(laps):
            laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]

        xsamples = xworkout.findall("pwx:sample", namespaces=ns)

        currentLapIdx = 0
        for xsample in xsamples:
            wp = Waypoint()
            wp.Timestamp = activity.StartTime + timedelta(seconds=float(xsample.find("pwx:timeoffset", namespaces=ns).text))

            # Just realized how terribly inefficient doing the search-if-set pattern is. I'll change everything over to iteration... eventually
            for xsampleData in xsample:
                tag = xsampleData.tag[34:] # {http://www.peaksware.com/PWX/1/0} is 34 chars. I'll show myself out.
                if tag == "hr":
                    wp.HR = int(xsampleData.text)
                elif tag == "spd":
                    wp.Speed = float(xsampleData.text)
                elif tag == "pwr":
                    wp.Power = float(xsampleData.text)
                elif tag == "cad":
                    wp.Cadence = int(xsampleData.text)
                elif tag == "dist":
                    wp.Distance = float(xsampleData.text)
                elif tag == "temp":
                    wp.Temp = float(xsampleData.text)
                elif tag == "alt":
                    if wp.Location is None:
                        wp.Location = Location()
                    wp.Location.Altitude = float(xsampleData.text)
                elif tag == "lat":
                    if wp.Location is None:
                        wp.Location = Location()
                    wp.Location.Latitude = float(xsampleData.text)
                elif tag == "lon":
                    if wp.Location is None:
                        wp.Location = Location()
                    wp.Location.Longitude = float(xsampleData.text)

            assert wp.Location is None or ((wp.Location.Latitude is None) == (wp.Location.Longitude is None)) # You never know...

            if wp.Location and wp.Location.Latitude is not None:
                activity.GPS = True

            # If we've left one lap, move to the next immediately
            while currentLapIdx < len(laps) - 1 and wp.Timestamp > laps[currentLapIdx].EndTime:
                currentLapIdx += 1

            laps[currentLapIdx].Waypoints.append(wp)
        activity.Laps = laps
        activity.Stationary = activity.CountTotalWaypoints() == 0
        if not activity.Stationary:
            flatWp = activity.GetFlatWaypoints()
            flatWp[0].Type = WaypointType.Start
            flatWp[-1].Type = WaypointType.End
            if activity.EndTime < flatWp[-1].Timestamp: # Work around the fact that TP doesn't preserve elapsed time.
                activity.EndTime = flatWp[-1].Timestamp
        return activity

    def Dump(activity):
        xroot = etree.Element("pwx", nsmap=PWXIO.Namespaces)

        xroot.attrib["creator"] = "tapiriik"
        xroot.attrib["version"] = "1.0"

        xworkout = etree.SubElement(xroot, "workout")

        if activity.Type in PWXIO._reverseSportTypeMappings:
            etree.SubElement(xworkout, "sportType").text = PWXIO._reverseSportTypeMappings[activity.Type]

        if activity.Name:
            etree.SubElement(xworkout, "title").text = activity.Name

        if activity.Notes:
            etree.SubElement(xworkout, "cmt").text = activity.Notes

        xdevice = etree.SubElement(xworkout, "device")

        # By Ben's request
        etree.SubElement(xdevice, "make").text = "tapiriik"
        if hasattr(activity, "SourceConnection"):
            etree.SubElement(xdevice, "model").text = activity.SourceConnection.Service.ID

        etree.SubElement(xworkout, "time").text = activity.StartTime.replace(tzinfo=None).isoformat()

        def _writeMinMaxAvg(xparent, name, stat, naturalValue=False):
            if stat.Min is None and stat.Max is None and stat.Average is None:
                return
            xstat = etree.SubElement(xparent, name)
            if stat.Min is not None:
                xstat.attrib["min"] = str(stat.Min)
            if stat.Max is not None:
                xstat.attrib["max"] = str(stat.Max)
            if stat.Average is not None:
                xstat.attrib["avg"] = str(stat.Average)

        def _writeSummaryData(xparent, obj, time_ref):
            xsummary = etree.SubElement(xparent, "summarydata")
            etree.SubElement(xsummary, "beginning").text = str((obj.StartTime - time_ref).total_seconds())
            etree.SubElement(xsummary, "duration").text = str((obj.EndTime - obj.StartTime).total_seconds())

            if obj.Stats.TimerTime.Value is not None:
                etree.SubElement(xsummary, "durationstopped").text = str((obj.EndTime - obj.StartTime).total_seconds() - obj.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value)

            altStat = obj.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters)

            _writeMinMaxAvg(xsummary, "hr", obj.Stats.HR.asUnits(ActivityStatisticUnit.BeatsPerMinute))
            _writeMinMaxAvg(xsummary, "spd", obj.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond))
            _writeMinMaxAvg(xsummary, "pwr", obj.Stats.Power.asUnits(ActivityStatisticUnit.Watts))
            if obj.Stats.Cadence.Min is not None or obj.Stats.Cadence.Max is not None or obj.Stats.Cadence.Average is not None:
                _writeMinMaxAvg(xsummary, "cad", obj.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute))
            else:
                _writeMinMaxAvg(xsummary, "cad", obj.Stats.RunCadence.asUnits(ActivityStatisticUnit.StepsPerMinute))
            if obj.Stats.Distance.Value:
                etree.SubElement(xsummary, "dist").text = str(obj.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
            _writeMinMaxAvg(xsummary, "alt", altStat)
            _writeMinMaxAvg(xsummary, "temp", obj.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius))

            if altStat.Gain is not None:
                etree.SubElement(xsummary, "climbingelevation").text = str(altStat.Gain)
            if altStat.Loss is not None:
                etree.SubElement(xsummary, "descendingelevation").text = str(altStat.Loss)

        _writeSummaryData(xworkout, activity, time_ref=activity.StartTime)

        for lap in activity.Laps:
            xsegment = etree.SubElement(xworkout, "segment")
            _writeSummaryData(xsegment, lap, time_ref=activity.StartTime)

        for wp in activity.GetFlatWaypoints():
            xsample = etree.SubElement(xworkout, "sample")
            etree.SubElement(xsample, "timeoffset").text = str((wp.Timestamp - activity.StartTime).total_seconds())

            if wp.HR is not None:
                etree.SubElement(xsample, "hr").text = str(round(wp.HR))

            if wp.Speed is not None:
                etree.SubElement(xsample, "spd").text = str(wp.Speed)

            if wp.Power is not None:
                etree.SubElement(xsample, "pwr").text = str(round(wp.Power))

            if wp.Cadence is not None:
                etree.SubElement(xsample, "cad").text = str(round(wp.Cadence))
            else:
                if wp.RunCadence is not None:
                    etree.SubElement(xsample, "cad").text = str(round(wp.RunCadence))

            if wp.Distance is not None:
                etree.SubElement(xsample, "dist").text = str(wp.Distance)

            if wp.Location is not None:
                if wp.Location.Longitude is not None:
                    etree.SubElement(xsample, "lat").text = str(wp.Location.Latitude)
                    etree.SubElement(xsample, "lon").text = str(wp.Location.Longitude)
                if wp.Location.Altitude is not None:
                    etree.SubElement(xsample, "alt").text = str(wp.Location.Altitude)

            if wp.Temp is not None:
                etree.SubElement(xsample, "temp").text = str(wp.Temp)


        return etree.tostring(xroot, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("UTF-8")
