from lxml import etree, objectify
from pytz import UTC
import copy
import dateutil.parser
from datetime import datetime
from .interchange import WaypointType, Activity, ActivityStatistic, ActivityStatisticUnit, ActivityType, Waypoint, Location
from .statistic_calculator import ActivityStatisticCalculator


class TCXIO:
    Namespaces = {
        None: "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
        "ns2": "http://www.garmin.com/xmlschemas/UserProfile/v2",
        "tpx": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
        "ns4": "http://www.garmin.com/xmlschemas/ProfileExtension/v1",
        "ns5": "http://www.garmin.com/xmlschemas/ActivityGoals/v1",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    }

    def Parse(tcxData, act=None):
        ns = copy.deepcopy(TCXIO.Namespaces)
        ns["tcx"] = ns[None]
        del ns[None]

        act = act if act else Activity()

        try:
            root = etree.XML(tcxData)
        except:
            root = etree.fromstring(tcxData)


        xacts = root.find("tcx:Activities", namespaces=ns)
        if xacts is None:
            raise ValueError("No activities element in TCX")

        xact = xacts.find("tcx:Activity", namespaces=ns)
        if xact is None:
            raise ValueError("No activity element in TCX")

        if not act.Type:
            if xact.attrib["Sport"] == "Biking":
                act.Type = ActivityType.Cycling
            elif xact.attrib["Sport"] == "Running":
                act.Type = ActivityType.Running

        xlaps = xact.findall("tcx:Lap", namespaces=ns)
        startTime = None
        endTime = None

        for xlap in xlaps:
            xtrkseg = xlap.find("tcx:Track", namespaces=ns)

            lap = Lap()
            activity.Laps.append(lap)

            lap.Stats.MovingTime.Value = timedelta(seconds=float(xlap.find("tcx:TotalTimeSeconds", namespaces=ns).text))
            lap.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, float(xlap.find("tcx:DistanceMeters", namespaces=ns).text))
            lap.Stats.Calories.Value = float(xlap.find("tcx:Calories", namespaces=ns).text)
            if lap.Stats.Calories == 0:
                lap.Stats.Calories = None # It's dumb to make this required, but I digress.
            lap.Intensity = LapIntensity.Active if xlap.find("tcx:Intensity", namespaces=ns).text == "Active" else LapIntensity.Rest
            lap.Trigger = ({
                "Manual": LapTriggerMethod.Manual,
                "Distance": LapTriggerMethod.Distance,
                "Location": LapTriggerMethod.PositionMarked,
                "Time": LapTriggerMethod.Time,
                "HeartRate": LapTriggerMethod.Manual # I guess - no equivalent in FIT
                })[xlap.find("tcx:TriggerMethod", namespaces=ns).text]

            maxSpdEl = xlap.find("tcx:MaximumSpeed", namespaces=ns)
            if maxSpdEl is not None:
                lap.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, max=float(maxSpdEl.text))

            avgHREl = xlap.find("tcx:AverageHeartRateBpm", namespaces=ns)
            if avgHREl is not None:
                lap.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(avgHREl.find("tcx:Value", namespaces=ns).text))

            maxHREl = xlap.find("tcx:MaximumHeartRateBpm", namespaces=ns)
            if maxHREl is not None:
                lap.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=float(maxHREl.find("tcx:Value", namespaces=ns).text)))

            # WF fills these in with invalid values.
            lap.Stats.HR.Max = lap.Stats.HR.Max if lap.Stats.HR.Max > 10 else None
            lap.Stats.HR.Average = lap.Stats.HR.Average if lap.Stats.HR.Average > 10 else None

            cadEl = xlap.find("tcx:Cadence", namespaces=ns)
            if cadEl is not None:
                lap.Stats.Cadence = ActivityStatistic(ActivityStatistic.RevolutionsPerMinute, avg=float(cadEl.text))

            extsEl = xlap.find("tcx:Extensions", namespaces=ns)
            if extsEl is not None:
                tpxEl = extsEl.find("tpx:TPX", namespaces=ns)
                if tpxEl is not None:
                    maxBikeCadEl = tpxEl.find("tpx:MaxBikeCadence", namespaces=ns)
                    if maxBikeCadEl is not None:
                        lap.Stats.Cadence.update(ActivityStatistic(ActivityStatistic.RevolutionsPerMinute, max=float(maxBikeCadEl.text)))
                    maxRunCadEl = tpxEl.find("tpx:MaxRunCadence", namespaces=ns)
                    if maxRunCadEl is not None:
                        lap.Stats.RunCadence.update(ActivityStatistic(ActivityStatistic.StepsPerMinute, max=float(maxRunCadEl.text)))
                    avgRunCadEl = tpxEl.find("tpx:AvgRunCadence", namespaces=ns)
                    if avgRunCadEl is not None:
                        lap.Stats.RunCadence.update(ActivityStatistic(ActivityStatistic.StepsPerMinute, avg=float(avgRunCadEl.text)))
                    stepsEl = tpxEl.find("tpx:Steps", namespaces=ns)
                    if stepsEl is not None:
                        lap.Stats.Strides.update(ActivityStatistic(ActivityStatistic.Strides, value=int(stepsEl.text)))

            if xtrkseg is None:
                # Some TCX files have laps with no track - not sure if it's valid or not.
                continue
            for xtrkpt in xtrkseg.findall("tcx:Trackpoint", namespaces=ns):
                wp = Waypoint()
                if len(act.Waypoints) == 0:
                    wp.Type = WaypointType.Start

                wp.Timestamp = dateutil.parser.parse(xtrkpt.find("tcx:Time", namespaces=ns).text)
                wp.Timestamp.replace(tzinfo=UTC)
                if startTime is None or wp.Timestamp < startTime:
                    startTime = wp.Timestamp
                if endTime is None or wp.Timestamp > endTime:
                    endTime = wp.Timestamp
                xpos = xtrkpt.find("tcx:Position", namespaces=ns)
                if xpos is not None:
                    wp.Location = Location(float(xpos.find("tcx:LatitudeDegrees", namespaces=ns).text), float(xpos.find("tcx:LongitudeDegrees", namespaces=ns).text), None)
                eleEl = xtrkpt.find("tcx:AltitudeMeters", namespaces=ns)
                if eleEl is not None:
                    wp.Location = wp.Location if wp.Location else Location(None, None, None)
                    wp.Location.Altitude = float(eleEl.text)
                distEl = xtrkpt.find("tcx:DistanceMeters", namespaces=ns)
                if distEl is not None:
                    wp.Distance = float(distEl.text)

                hrEl = xtrkpt.find("tcx:HeartRateBpm", namespaces=ns)
                if hrEl is not None:
                    wp.HR = int(hrEl.find("tcx:Value", namespaces=ns).text)
                cadEl = xtrkpt.find("tcx:Cadence", namespaces=ns)
                if cadEl is not None:
                    wp.Cadence = int(cadEl.text)
                extsEl = xtrkpt.find("tcx:Extensions", namespaces=ns)
                if extsEl is not None:
                    tpxEl = extsEl.find("tpx:TPX", namespaces=ns)
                    if tpxEl is not None:
                        powerEl = tpxEl.find("tpx:Watts", namespaces=ns)
                        if powerEl is not None:
                            wp.Power = float(powerEl.text)
                        runCadEl = tpxEl.find("tpx:RunCadence", namespaces=ns)
                        if runCadEl is not None:
                            wp.RunCadence = float(runCadEl.text)
                lap.Waypoints.append(wp)
                xtrkpt.clear()
                del xtrkpt

        if len(act.Laps):
            if len(act.Laps[-1].Waypoints):
                act.Laps[-1].Waypoints[-1].Type = WaypointType.End
            act.TZ = UTC
            act.Stats.Distance.Value = sum([x.Stats.Distance.Value for x in act.Laps])
            act.StartTime = startTime
            act.EndTime = endTime
            act.CalculateUID()

        return act

    def Dump(activity):

        TRKPTEXT = "{%s}" % TCXIO.Namespaces["tpx"]
        root = etree.Element("TrainingCenterDatabase", nsmap=TCXIO.Namespaces)
        activities = etree.SubElement(root, "Activities")
        act = etree.SubElement(activities, "Activity")


        author = etree.SubElement(root, "Author")
        author.attrib["{" + TCXIO.Namespaces["xsi"] + "}type"] = "Application_t"
        etree.SubElement(author, "Name").text = "tapiriik"
        build = etree.SubElement(author, "Build")
        version = etree.SubElement(build, "Version")
        etree.SubElement(version, "VersionMajor").text = "0"
        etree.SubElement(version, "VersionMinor").text = "0"
        etree.SubElement(version, "BuildMajor").text = "0"
        etree.SubElement(version, "BuildMinor").text = "0"
        etree.SubElement(author, "LangID").text = "en"
        etree.SubElement(author, "PartNumber").text = "000-00000-00"

        dateFormat = "%Y-%m-%dT%H:%M:%S.000Z"

        if activity.Name is not None:
            etree.SubElement(act, "Notes").text = activity.Name

        if activity.Type == ActivityType.Cycling:
            act.attrib["Sport"] = "Biking"
        elif activity.Type == ActivityType.Running:
            act.attrib["Sport"] = "Running"
        else:
            act.attrib["Sport"] = "Other"

        etree.SubElement(act, "Id").text = activity.StartTime.astimezone(UTC).strftime(dateFormat)

        xlaps = []
        for lap in activity.Laps:
            xlap = etree.SubElement(act, "Lap")
            xlaps.append(xlap)
            etree.SubElement(xlap, "Intensity").text = "Resting" if lap.Intensity == LapIntensity.Active else "Active"
            etree.SubElement(xlap, "TriggerMethod").text = ({
                LapTriggerMethod.Manual: "Manual",
                LapTriggerMethod.Distance: "Distance",
                LapTriggerMethod.PositionMarked: "Location",
                LapTriggerMethod.Time: "Time",
                LapTriggerMethod.PositionStart: "Location",
                LapTriggerMethod.PositionLap: "Location",
                LapTriggerMethod.PositionMarked: "Location",
                LapTriggerMethod.SessionEnd: "Manual",
                LapTriggerMethod.FitnessEquipment: "Manual"
                })[lap.TriggerMethod]

            xlap.attrib["StartTime"] = lap.StartTime.astimezone(UTC).strftime(dateFormat)
            def _writeStat(parent, elName, value, wrapValue=False, naturalValue=False):
                if value is not None:
                    xstat = etree.SubElement(parent, elName)
                    if wrapValue:
                        xstat = etree.SubElement("tcx:Value", xstat)
                    xstat.text = str(value) if not naturalValue else str(int(value))

            _writeStat(xlap, "tcx:MaximumSpeed", lap.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Max)
            _writeStat(xlap, "tcx:AverageHeartRateBpm", lap.Stats.HR.Average, naturalValue=True, wrapValue=True)
            _writeStat(xlap, "tcx:MaximumHeartRateBpm", lap.Stats.HR.Max, naturalValue=True, wrapValue=True)
            _writeStat(xlap, "tcx:Cadence", lap.Stats.Cadence.Average, naturalValue=True)
            _writeStat(xlap, "txc:DistanceMeters", lap.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
            _writeStat(xlap, "txc:TotalTimeSeconds", lap.Stats.MovingTime.Value.total_seconds() if lap.Stats.MovingTime.Value else None)

            if lap.Stats.Cadence.Max is not None or lap.Stats.RunCadence.Max is not None or lap.Stats.RunCadence.Average  is not None or lap.Stats.Strides.Value is not None:
                exts = etree.SubElement(xlap, "Extensions")
                lapext = etree.SubElement(exts, TRKPTEXT + "LX")
                lapext.attrib["xmlns"] = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                _writeStat(lapext, "MaxBikeCadence", lap.Stats.Cadence.Max, naturalValue=True)
                _writeStat(lapext, "MaxRunCadence", lap.Stats.RunCadence.Max, naturalValue=True)
                _writeStat(lapext, "AvgRunCadence", lap.Stats.RunCadence.Average, naturalValue=True)
                _writeStat(lapext, "Steps", lap.Stats.Steps.Valie, naturalValue=True)

        inPause = False
        for lap in activity.Laps:
            xlap = xlaps[activity.Laps.index(lap)]
            track = None
            for wp in lap.Waypoints:
                if wp.Location is None or wp.Location.Latitude is None or wp.Location.Longitude is None:
                    continue  # drop the point
                if wp.Type == WaypointType.Pause:
                    if inPause:
                        continue  # this used to be an exception, but I don't think that was merited
                    inPause = True
                if inPause and wp.Type != WaypointType.Pause or wp.Type == WaypointType.Lap:
                    inPause = False
                if track is None:  # Defer creating the track until there are points
                    track = etree.SubElement(xlap, "Track") # TODO - pauses should create new tracks instead of new laps?
                trkpt = etree.SubElement(track, "Trackpoint")
                if wp.Timestamp.tzinfo is None:
                    raise ValueError("TCX export requires TZ info")
                etree.SubElement(trkpt, "Time").text = wp.Timestamp.astimezone(UTC).strftime(dateFormat)
                if wp.Location:
                    pos = etree.SubElement(trkpt, "Position")
                    etree.SubElement(pos, "LatitudeDegrees").text = str(wp.Location.Latitude)
                    etree.SubElement(pos, "LongitudeDegrees").text = str(wp.Location.Longitude)

                    if wp.Location.Altitude is not None:
                        etree.SubElement(trkpt, "AltitudeMeters").text = str(wp.Location.Altitude)
                    if wp.Distance is not None:
                        etree.SubElement(trkpt, "DistanceMeters").text = str(wp.Distance)
                    if wp.HR is not None:
                        xhr = etree.SubElement(trkpt, "HeartRateBpm")
                        xhr.attrib["{" + TCXIO.Namespaces["xsi"] + "}type"] = "HeartRateInBeatsPerMinute_t"
                        etree.SubElement(xhr, "Value").text = str(int(wp.HR))
                    if wp.Cadence is not None:
                        etree.SubElement(trkpt, "Cadence").text = str(int(wp.Cadence))
                    if wp.Power is not None or wp.RunCadence is not None:
                        exts = etree.SubElement(trkpt, "Extensions")
                        gpxtpxexts = etree.SubElement(exts, "TPX")
                        gpxtpxexts.attrib["xmlns"] = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                        if wp.Power is not None:
                            etree.SubElement(gpxtpxexts, "Watts").text = str(int(wp.Power))
                        if wp.RunCadence is not None:
                            etree.SubElement(gpxtpxexts, "RunCadence").text = str(int(wp.RunCadence))
        return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("UTF-8")
