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

        beginSeg = False
        for xlap in xlaps:
            beginSeg = True
            xtrkseg = xlap.find("tcx:Track", namespaces=ns)
            if xtrkseg is None:
                # Some TCX files have laps with no track - not sure if it's valid or not.
                continue
            for xtrkpt in xtrkseg.findall("tcx:Trackpoint", namespaces=ns):
                wp = Waypoint()
                if len(act.Waypoints) == 0:
                    wp.Type = WaypointType.Start
                elif beginSeg:
                    wp.Type = WaypointType.Lap
                beginSeg = False

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
                act.Waypoints.append(wp)
                xtrkpt.clear()
                del xtrkpt

        if len(act.Waypoints):
            act.Waypoints[len(act.Waypoints)-1].Type = WaypointType.End
            act.TZ = act.Waypoints[0].Timestamp.tzinfo
            act.Stats.Distance.Value = ActivityStatisticCalculator.CalculateDistance(act)
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
        lap = track = None
        inPause = False
        lapStartWpt = None
        if len(activity.Waypoints): # GPS activity.
            def newLap(wpt):
                nonlocal lapStartWpt, lap, track
                lapStartWpt = wpt
                lap = etree.SubElement(act, "Lap")
                if wpt: # If not, statistics will be manually inserted.
                    lap.attrib["StartTime"] = wpt.Timestamp.astimezone(UTC).strftime(dateFormat)
                    if wpt.Calories and lapStartWpt.Calories:
                        etree.SubElement(lap, "Calories").text = str(wpt.Calories - lapStartWpt.Calories)
                    else:
                        etree.SubElement(lap, "Calories").text = "0"  # meh schema is meh
                etree.SubElement(lap, "Intensity").text = "Active"
                etree.SubElement(lap, "TriggerMethod").text = "Manual"  # I assume!

                track = None

            def finishLap(wpt):
                nonlocal lapStartWpt, lap
                if not wpt:
                    return # Well, this was useful.
                dist = ActivityStatisticCalculator.CalculateDistance(activity, lapStartWpt, wpt)
                movingTime = ActivityStatisticCalculator.CalculateMovingTime(activity, lapStartWpt, wpt)
                xdist = etree.SubElement(lap, "DistanceMeters")
                xdist.text = str(dist)
                totaltime = etree.SubElement(lap, "TotalTimeSeconds")
                totaltime.text = str(movingTime.total_seconds())
                lap.insert(0, xdist)
                lap.insert(0, totaltime)

            newLap(activity.Waypoints[0])
            for wp in activity.Waypoints:
                if wp.Location is None or wp.Location.Latitude is None or wp.Location.Longitude is None:
                    continue  # drop the point
                if wp.Type == WaypointType.Pause:
                    if inPause:
                        continue  # this used to be an exception, but I don't think that was merited
                    inPause = True
                if inPause and wp.Type != WaypointType.Pause or wp.Type == WaypointType.Lap:
                    # Make a new lap when they unpause
                    inPause = False
                    finishLap(wp)
                    newLap(wp)
                if track is None:  # Defer creating the track until there are points
                    track = etree.SubElement(lap, "Track") # TODO - pauses should create new tracks instead of new laps?
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
                    if wp.HR is not None:
                        xhr = etree.SubElement(trkpt, "HeartRateBpm")
                        xhr.attrib["{" + TCXIO.Namespaces["xsi"] + "}type"] = "HeartRateInBeatsPerMinute_t"
                        etree.SubElement(xhr, "Value").text = str(int(wp.HR))
                    if wp.Cadence is not None:
                        etree.SubElement(trkpt, "Cadence").text = str(int(wp.Cadence))
                    if wp.Power is not None:
                        exts = etree.SubElement(trkpt, "Extensions")
                        gpxtpxexts = etree.SubElement(exts, "TPX")
                        gpxtpxexts.attrib["xmlns"] = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
                        etree.SubElement(gpxtpxexts, "Watts").text = str(int(wp.Power))
                finishLap(wp)
        else: # Stationary activity.
            newLap(None)
            # `lap` is now our active lap - fill it in with all the statistics we can.

            totaltime = etree.SubElement(lap, "TotalTimeSeconds")
            totaltime.text = str((act.EndTime - act.StartTime).total_seconds())

            lap.attrib["StartTime"] = act.StartTime.astimezone(UTC).strftime(dateFormat)

            dist = act.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value
            if dist is not None:
                xdist = etree.SubElement(lap, "DistanceMeters")
                xdist.text = str(dist)

            kcal = act.Stats.Kilocalories.asUnits(ActivityStatisticUnit.Kilocalories).Value
            if kcal is not None:
                xcal = etree.SubElement(lap, "Calories")
                xcal.text = str(int(kcal))

            avgcad = act.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Average
            if avgcad is not None:
                xavgcad = etree.SubElement(lap, "Cadence")
                xavgcad.text = str(int(avgcad))


            avghr = act.Stats.HR.asUnits(ActivityStatisticUnit.BeatsPerMinute).Value
            if avghr is not None:
                xavghr = etree.SubElement(lap, "AverageHeartRateBpm")
                xavghrval = etree.SubElement(xavghr, "Value")
                xavghrval.text = str(int(avghr))

            maxhr = act.Stats.HR.asUnits(ActivityStatisticUnit.BeatsPerMinute).Max
            if maxhr is not None:
                xmaxhr = etree.SubElement(lap, "MaximumHeartRateBpm")
                xmaxhrval = etree.SubElement(xmaxhr, "Value")
                xmaxhrval.text = str(int(maxhr))

            avgspeed = act.Stats.Speed.asUnits(ActivityStatisticUnit.KilometersPerHour).Value
            maxcad = act.Stats.Cadence.asUnits(ActivityStatisticUnit.RevolutionsPerMinute).Max
            if avgspeed is not None or maxcad is not None or avgcad is not None:
                exts = etree.SubElement(lap, "Extensions")
                lapext = etree.SubElement(exts, TRKPTEXT + "LX")
                if avgspeed is not None:
                    etree.SubElement(lapext, TRKPTEXT + "AvgSpeed").text = str(avgspeed)
                if maxcad is not None:
                    etree.SubElement(lapext, TRKPTEXT + "MaxBikeCadence").text = str(int(maxcad))
                    etree.SubElement(lapext, TRKPTEXT + "MaxRunCadence").text = str(int(maxcad)) # I'll probably never know the point of having these seperate
                if avgcad is not None:
                    etree.SubElement(lapext, TRKPTEXT + "AvgRunCadence").text = str(int(avgcad)) # The TCX schema specifically states that "AvgBikeCadence" is actually put into a Cadence element under Lap

            finishLap(None)
        return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("UTF-8")
