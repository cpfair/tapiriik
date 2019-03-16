from lxml import etree
from pytz import UTC
import copy
import dateutil.parser
from datetime import timedelta
from .interchange import WaypointType, Activity, ActivityStatistic, ActivityStatistics, ActivityStatisticUnit, ActivityType, Waypoint, Location, Lap, LapIntensity, LapTriggerMethod
from .devices import DeviceIdentifier, DeviceIdentifierType, Device


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

        act.GPS = False

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

        if not act.Type or act.Type == ActivityType.Other:
            if xact.attrib["Sport"] == "Biking":
                act.Type = ActivityType.Cycling
            elif xact.attrib["Sport"] == "Running":
                act.Type = ActivityType.Running

        xnotes = xact.find("tcx:Notes", namespaces=ns)
        if xnotes is not None and xnotes.text:
            xnotes_lines = xnotes.text.splitlines()
            act.Name = xnotes_lines[0]
            if len(xnotes_lines) > 1:
                act.Notes = '\n'.join(xnotes_lines[1:])

        xcreator = xact.find("tcx:Creator", namespaces=ns)
        if xcreator is not None and xcreator.attrib["{" + TCXIO.Namespaces["xsi"] + "}type"] == "Device_t":
            devId = DeviceIdentifier.FindMatchingIdentifierOfType(DeviceIdentifierType.TCX, {"ProductID": int(xcreator.find("tcx:ProductID", namespaces=ns).text)}) # Who knows if this is unique in the TCX ecosystem? We'll find out!
            xver = xcreator.find("tcx:Version", namespaces=ns)
            verMaj = None
            verMin = None
            if xver is not None:
                verMaj = int(xver.find("tcx:VersionMajor", namespaces=ns).text)
                verMin = int(xver.find("tcx:VersionMinor", namespaces=ns).text)
            act.Device = Device(devId, int(xcreator.find("tcx:UnitId", namespaces=ns).text), verMaj=verMaj, verMin=verMin) # ID vs Id: ???

        xlaps = xact.findall("tcx:Lap", namespaces=ns)
        startTime = None
        endTime = None
        for xlap in xlaps:

            lap = Lap()
            act.Laps.append(lap)

            lap.StartTime = dateutil.parser.parse(xlap.attrib["StartTime"])
            totalTimeEL = xlap.find("tcx:TotalTimeSeconds", namespaces=ns)
            if totalTimeEL is None:
                raise ValueError("Missing lap TotalTimeSeconds")
            lap.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, float(totalTimeEL.text))

            lap.EndTime = lap.StartTime + timedelta(seconds=float(totalTimeEL.text))

            distEl = xlap.find("tcx:DistanceMeters", namespaces=ns)
            energyEl = xlap.find("tcx:Calories", namespaces=ns)
            triggerEl = xlap.find("tcx:TriggerMethod", namespaces=ns)
            intensityEl = xlap.find("tcx:Intensity", namespaces=ns)

            # Some applications slack off and omit these, despite the fact that they're required in the spec.
            # I will, however, require lap distance, because, seriously.
            if distEl is None:
                raise ValueError("Missing lap DistanceMeters")

            lap.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, float(distEl.text))
            if energyEl is not None and energyEl.text:
                lap.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, float(energyEl.text))
                if lap.Stats.Energy.Value == 0:
                    lap.Stats.Energy.Value = None # It's dumb to make this required, but I digress.

            if intensityEl is not None:
                lap.Intensity = LapIntensity.Active if intensityEl.text == "Active" else LapIntensity.Rest
            else:
                lap.Intensity = LapIntensity.Active

            if triggerEl is not None:
                lap.Trigger = ({
                    "Manual": LapTriggerMethod.Manual,
                    "Distance": LapTriggerMethod.Distance,
                    "Location": LapTriggerMethod.PositionMarked,
                    "Time": LapTriggerMethod.Time,
                    "HeartRate": LapTriggerMethod.Manual # I guess - no equivalent in FIT
                    })[triggerEl.text]
            else:
                lap.Trigger = LapTriggerMethod.Manual # One would presume

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
            lap.Stats.HR.Max = lap.Stats.HR.Max if lap.Stats.HR.Max and lap.Stats.HR.Max > 10 else None
            lap.Stats.HR.Average = lap.Stats.HR.Average if lap.Stats.HR.Average and lap.Stats.HR.Average > 10 else None

            cadEl = xlap.find("tcx:Cadence", namespaces=ns)
            if cadEl is not None:
                lap.Stats.Cadence = ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, avg=float(cadEl.text))

            extsEl = xlap.find("tcx:Extensions", namespaces=ns)
            if extsEl is not None:
                lxEls = extsEl.findall("tpx:LX", namespaces=ns)
                for lxEl in lxEls:
                    avgSpeedEl = lxEl.find("tpx:AvgSpeed", namespaces=ns)
                    if avgSpeedEl is not None:
                        lap.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=float(avgSpeedEl.text)))
                    maxBikeCadEl = lxEl.find("tpx:MaxBikeCadence", namespaces=ns)
                    if maxBikeCadEl is not None:
                        lap.Stats.Cadence.update(ActivityStatistic(ActivityStatisticUnit.RevolutionsPerMinute, max=float(maxBikeCadEl.text)))
                    maxPowerEl = lxEl.find("tpx:MaxWatts", namespaces=ns)
                    if maxPowerEl is not None:
                        lap.Stats.Power.update(ActivityStatistic(ActivityStatisticUnit.Watts, max=float(maxPowerEl.text)))
                    avgPowerEl = lxEl.find("tpx:AvgWatts", namespaces=ns)
                    if avgPowerEl is not None:
                        lap.Stats.Power.update(ActivityStatistic(ActivityStatisticUnit.Watts, avg=float(avgPowerEl.text)))
                    maxRunCadEl = lxEl.find("tpx:MaxRunCadence", namespaces=ns)
                    if maxRunCadEl is not None:
                        lap.Stats.RunCadence.update(ActivityStatistic(ActivityStatisticUnit.StepsPerMinute, max=float(maxRunCadEl.text)))
                    avgRunCadEl = lxEl.find("tpx:AvgRunCadence", namespaces=ns)
                    if avgRunCadEl is not None:
                        lap.Stats.RunCadence.update(ActivityStatistic(ActivityStatisticUnit.StepsPerMinute, avg=float(avgRunCadEl.text)))
                    stepsEl = lxEl.find("tpx:Steps", namespaces=ns)
                    if stepsEl is not None:
                        lap.Stats.Strides.update(ActivityStatistic(ActivityStatisticUnit.Strides, value=float(stepsEl.text)))

            xtrkseg = xlap.find("tcx:Track", namespaces=ns)
            if xtrkseg is None:
                # Some TCX files have laps with no track - not sure if it's valid or not.
                continue
            for xtrkpt in xtrkseg.findall("tcx:Trackpoint", namespaces=ns):
                wp = Waypoint()
                tsEl = xtrkpt.find("tcx:Time", namespaces=ns)
                if tsEl is None:
                    raise ValueError("Trackpoint without timestamp")
                wp.Timestamp = dateutil.parser.parse(tsEl.text)
                wp.Timestamp.replace(tzinfo=UTC)
                if startTime is None or wp.Timestamp < startTime:
                    startTime = wp.Timestamp
                if endTime is None or wp.Timestamp > endTime:
                    endTime = wp.Timestamp
                xpos = xtrkpt.find("tcx:Position", namespaces=ns)
                if xpos is not None:
                    act.GPS = True
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
                    wp.HR = float(hrEl.find("tcx:Value", namespaces=ns).text)
                cadEl = xtrkpt.find("tcx:Cadence", namespaces=ns)
                if cadEl is not None:
                    wp.Cadence = float(cadEl.text)
                extsEl = xtrkpt.find("tcx:Extensions", namespaces=ns)
                if extsEl is not None:
                    tpxEl = extsEl.find("tpx:TPX", namespaces=ns)
                    if tpxEl is not None:
                        powerEl = tpxEl.find("tpx:Watts", namespaces=ns)
                        if powerEl is not None:
                            wp.Power = float(powerEl.text)
                        speedEl = tpxEl.find("tpx:Speed", namespaces=ns)
                        if speedEl is not None:
                            wp.Speed = float(speedEl.text)
                        runCadEl = tpxEl.find("tpx:RunCadence", namespaces=ns)
                        if runCadEl is not None:
                            wp.RunCadence = float(runCadEl.text)
                lap.Waypoints.append(wp)
                xtrkpt.clear()
                del xtrkpt
            if len(lap.Waypoints):
                lap.EndTime = lap.Waypoints[-1].Timestamp

        act.StartTime = act.Laps[0].StartTime if len(act.Laps) else act.StartTime
        act.EndTime = act.Laps[-1].EndTime if len(act.Laps) else act.EndTime

        if act.CountTotalWaypoints():
            act.Stationary = False
            act.GetFlatWaypoints()[0].Type = WaypointType.Start
            act.GetFlatWaypoints()[-1].Type = WaypointType.End
        else:
            act.Stationary = True
        if len(act.Laps) == 1:
            act.Laps[0].Stats.update(act.Stats) # External source is authorative
            act.Stats = act.Laps[0].Stats
        else:
            sum_stats = ActivityStatistics() # Blank
            for lap in act.Laps:
                sum_stats.sumWith(lap.Stats)
            sum_stats.update(act.Stats)
            act.Stats = sum_stats

        act.PrerenderedFormats["tcx"] = tcxData

        act.CalculateUID()
        return act
    
    def Dump(activity, activityType=None):

        if "tcx" in activity.PrerenderedFormats:
            return activity.PrerenderedFormats["tcx"]

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

        if activityType:
            act.attrib["Sport"] = activityType
        elif activity.Type == ActivityType.Cycling:
            act.attrib["Sport"] = "Biking"
        elif activity.Type == ActivityType.Running:
            act.attrib["Sport"] = "Running"
        else:
            act.attrib["Sport"] = "Other"

        etree.SubElement(act, "Id").text = activity.StartTime.astimezone(UTC).strftime(dateFormat)

        def _writeStat(parent, elName, value, wrapValue=False, naturalValue=False, default=None):
                if value is not None or default is not None:
                    xstat = etree.SubElement(parent, elName)
                    if wrapValue:
                        xstat = etree.SubElement(xstat, "Value")
                    value = value if value is not None else default
                    xstat.text = str(value) if not naturalValue else str(int(value))

        xlaps = []
        for lap in activity.Laps:
            xlap = etree.SubElement(act, "Lap")
            xlaps.append(xlap)

            xlap.attrib["StartTime"] = lap.StartTime.astimezone(UTC).strftime(dateFormat)

            _writeStat(xlap, "TotalTimeSeconds", lap.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value if lap.Stats.TimerTime.Value else None, default=(lap.EndTime - lap.StartTime).total_seconds())
            _writeStat(xlap, "DistanceMeters", lap.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
            _writeStat(xlap, "MaximumSpeed", lap.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Max)
            _writeStat(xlap, "Calories", lap.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value, default=0, naturalValue=True)
            _writeStat(xlap, "AverageHeartRateBpm", lap.Stats.HR.Average, naturalValue=True, wrapValue=True)
            _writeStat(xlap, "MaximumHeartRateBpm", lap.Stats.HR.Max, naturalValue=True, wrapValue=True)

            etree.SubElement(xlap, "Intensity").text = "Resting" if lap.Intensity == LapIntensity.Rest else "Active"

            _writeStat(xlap, "Cadence", lap.Stats.Cadence.Average, naturalValue=True)

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
                })[lap.Trigger]

            if len([x for x in [lap.Stats.Cadence.Max, lap.Stats.RunCadence.Max, lap.Stats.RunCadence.Average, lap.Stats.Strides.Value, lap.Stats.Power.Max, lap.Stats.Power.Average, lap.Stats.Speed.Average] if x is not None]):
                exts = etree.SubElement(xlap, "Extensions")
                lapext = etree.SubElement(exts, "LX")
                lapext.attrib["xmlns"] = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                _writeStat(lapext, "MaxBikeCadence", lap.Stats.Cadence.Max, naturalValue=True)
                # This dividing-by-two stuff is getting silly
                _writeStat(lapext, "MaxRunCadence", lap.Stats.RunCadence.Max if lap.Stats.RunCadence.Max is not None else None, naturalValue=True)
                _writeStat(lapext, "AvgRunCadence", lap.Stats.RunCadence.Average if lap.Stats.RunCadence.Average is not None else None, naturalValue=True)
                _writeStat(lapext, "Steps", lap.Stats.Strides.Value, naturalValue=True)
                _writeStat(lapext, "MaxWatts", lap.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Max, naturalValue=True)
                _writeStat(lapext, "AvgWatts", lap.Stats.Power.asUnits(ActivityStatisticUnit.Watts).Average, naturalValue=True)
                _writeStat(lapext, "AvgSpeed", lap.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Average)

        inPause = False
        for lap in activity.Laps:
            xlap = xlaps[activity.Laps.index(lap)]
            track = None
            for wp in lap.Waypoints:
                if wp.Type == WaypointType.Pause:
                    if inPause:
                        continue  # this used to be an exception, but I don't think that was merited
                    inPause = True
                if inPause and wp.Type != WaypointType.Pause:
                    inPause = False
                if track is None:  # Defer creating the track until there are points
                    track = etree.SubElement(xlap, "Track") # TODO - pauses should create new tracks instead of new laps?
                trkpt = etree.SubElement(track, "Trackpoint")
                if wp.Timestamp.tzinfo is None:
                    raise ValueError("TCX export requires TZ info")
                etree.SubElement(trkpt, "Time").text = wp.Timestamp.astimezone(UTC).strftime(dateFormat)
                if wp.Location:
                    if wp.Location.Latitude is not None and wp.Location.Longitude is not None:
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
                if wp.Power is not None or wp.RunCadence is not None or wp.Speed is not None:
                    exts = etree.SubElement(trkpt, "Extensions")
                    gpxtpxexts = etree.SubElement(exts, "TPX")
                    gpxtpxexts.attrib["xmlns"] = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                    if wp.Speed is not None:
                        etree.SubElement(gpxtpxexts, "Speed").text = str(wp.Speed)
                    if wp.RunCadence is not None:
                        etree.SubElement(gpxtpxexts, "RunCadence").text = str(int(wp.RunCadence))
                    if wp.Power is not None:
                        etree.SubElement(gpxtpxexts, "Watts").text = str(int(wp.Power))
            if track is not None:
                exts = xlap.find("Extensions")
                if exts is not None:
                    track.addnext(exts)

        if activity.Name is not None and activity.Notes is not None:
            etree.SubElement(act, "Notes").text = '\n'.join((activity.Name, activity.Notes))
        elif activity.Name is not None:
            etree.SubElement(act, "Notes").text = activity.Name
        elif activity.Notes is not None:
            etree.SubElement(act, "Notes").text = '\n' + activity.Notes

        if activity.Device and activity.Device.Identifier:
            devId = DeviceIdentifier.FindEquivalentIdentifierOfType(DeviceIdentifierType.TCX, activity.Device.Identifier)
            if devId:
                xcreator = etree.SubElement(act, "Creator")
                xcreator.attrib["{" + TCXIO.Namespaces["xsi"] + "}type"] = "Device_t"
                etree.SubElement(xcreator, "Name").text = devId.Name
                etree.SubElement(xcreator, "UnitId").text = str(activity.Device.Serial) if activity.Device.Serial else "0"
                etree.SubElement(xcreator, "ProductID").text = str(devId.ProductID)
                xver = etree.SubElement(xcreator, "Version")
                etree.SubElement(xver, "VersionMajor").text = str(activity.Device.VersionMajor) if activity.Device.VersionMajor else "0" # Blegh.
                etree.SubElement(xver, "VersionMinor").text = str(activity.Device.VersionMinor) if activity.Device.VersionMinor else "0"
                etree.SubElement(xver, "BuildMajor").text = "0"
                etree.SubElement(xver, "BuildMinor").text = "0"

        return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("UTF-8")
