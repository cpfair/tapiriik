from lxml import etree
from pytz import UTC
import copy
import dateutil.parser
from datetime import datetime
from .interchange import WaypointType, Activity, Waypoint, Location, Lap, ActivityStatistic, ActivityStatisticUnit
from .statistic_calculator import ActivityStatisticCalculator

class GPXIO:
    Namespaces = {
        None: "http://www.topografix.com/GPX/1/1",
        "gpxtpx": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
        "gpxdata": "http://www.cluetrust.com/XML/GPXDATA/1/0",
        "gpxext": "http://www.garmin.com/xmlschemas/GpxExtensions/v3"
    }

    def Parse(gpxData, suppress_validity_errors=False, activity=None):
        ns = copy.deepcopy(GPXIO.Namespaces)
        ns["gpx"] = ns[None]
        del ns[None]
        act = Activity() if not activity else activity

        act.GPS = True # All valid GPX files have GPS data

        try:
            root = etree.XML(gpxData)
        except:
            root = etree.fromstring(gpxData)

        # GPSBabel produces files with the GPX/1/0 schema - I have no clue what's new in /1
        # So, blindly accept whatever we're given!
        ns["gpx"] = root.nsmap[None]

        xmeta = root.find("gpx:metadata", namespaces=ns)
        if xmeta is not None:
            xname = xmeta.find("gpx:name", namespaces=ns)
            if xname is not None:
                act.Name = xname.text
        xtrk = root.find("gpx:trk", namespaces=ns)

        if xtrk is None:
            raise ValueError("Invalid GPX")

        xtrksegs = xtrk.findall("gpx:trkseg", namespaces=ns)
        startTime = None
        endTime = None

        for xtrkseg in xtrksegs:
            lap = Lap()
            for xtrkpt in xtrkseg.findall("gpx:trkpt", namespaces=ns):
                wp = Waypoint()

                wp.Timestamp = dateutil.parser.parse(xtrkpt.find("gpx:time", namespaces=ns).text)
                wp.Timestamp.replace(tzinfo=UTC)
                if startTime is None or wp.Timestamp < startTime:
                    startTime = wp.Timestamp
                if endTime is None or wp.Timestamp > endTime:
                    endTime = wp.Timestamp

                wp.Location = Location(float(xtrkpt.attrib["lat"]), float(xtrkpt.attrib["lon"]), None)
                eleEl = xtrkpt.find("gpx:ele", namespaces=ns)
                if eleEl is not None:
                    wp.Location.Altitude = float(eleEl.text)
                extEl = xtrkpt.find("gpx:extensions", namespaces=ns)
                if extEl is not None:
                    gpxtpxExtEl = extEl.find("gpxtpx:TrackPointExtension", namespaces=ns)
                    if gpxtpxExtEl is not None:
                        hrEl = gpxtpxExtEl.find("gpxtpx:hr", namespaces=ns)
                        if hrEl is not None:
                            wp.HR = float(hrEl.text)
                        cadEl = gpxtpxExtEl.find("gpxtpx:cad", namespaces=ns)
                        if cadEl is not None:
                            wp.Cadence = float(cadEl.text)
                        tempEl = gpxtpxExtEl.find("gpxtpx:atemp", namespaces=ns)
                        if tempEl is not None:
                            wp.Temp = float(tempEl.text)
                    gpxdataHR = extEl.find("gpxdata:hr", namespaces=ns)
                    if gpxdataHR is not None:
                        wp.HR = float(gpxdataHR.text)
                    gpxdataCadence = extEl.find("gpxdata:cadence", namespaces=ns)
                    if gpxdataCadence is not None:
                        wp.Cadence = float(gpxdataCadence.text)
                lap.Waypoints.append(wp)
            act.Laps.append(lap)
            if not len(lap.Waypoints) and not suppress_validity_errors:
                raise ValueError("Track segment without points")
            elif len(lap.Waypoints):
                lap.StartTime = lap.Waypoints[0].Timestamp
                lap.EndTime = lap.Waypoints[-1].Timestamp

        if not len(act.Laps) and not suppress_validity_errors:
            raise ValueError("File with no track segments")

        if act.CountTotalWaypoints():
            act.GetFlatWaypoints()[0].Type = WaypointType.Start
            act.GetFlatWaypoints()[-1].Type = WaypointType.End
            act.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=ActivityStatisticCalculator.CalculateDistance(act))

            if len(act.Laps) == 1:
                # GPX encodes no real per-lap/segment statistics, so this is the only case where we can fill this in.
                # I've made an exception for the activity's total distance, but only because I want it later on for stats.
                act.Laps[0].Stats = act.Stats

        act.Stationary = False
        act.StartTime = startTime
        act.EndTime = endTime

        act.CalculateUID()
        return act

    def Dump(activity):
        GPXTPX = "{" + GPXIO.Namespaces["gpxtpx"] + "}"
        root = etree.Element("gpx", nsmap=GPXIO.Namespaces)
        root.attrib["creator"] = "tapiriik-sync"
        meta = etree.SubElement(root, "metadata")
        trk = etree.SubElement(root, "trk")
        if activity.Stationary:
            raise ValueError("Please don't use GPX for stationary activities.")
        if activity.Name is not None:
            etree.SubElement(meta, "name").text = activity.Name
            etree.SubElement(trk, "name").text = activity.Name

        inPause = False
        for lap in activity.Laps:
            trkseg = etree.SubElement(trk, "trkseg")
            for wp in lap.Waypoints:
                if wp.Location is None or wp.Location.Latitude is None or wp.Location.Longitude is None:
                    continue  # drop the point
                if wp.Type == WaypointType.Pause:
                    if inPause:
                        continue  # this used to be an exception, but I don't think that was merited
                    inPause = True
                if inPause and wp.Type != WaypointType.Pause:
                    inPause = False
                trkpt = etree.SubElement(trkseg, "trkpt")
                if wp.Timestamp.tzinfo is None:
                    raise ValueError("GPX export requires TZ info")
                etree.SubElement(trkpt, "time").text = wp.Timestamp.astimezone(UTC).isoformat()
                trkpt.attrib["lat"] = str(wp.Location.Latitude)
                trkpt.attrib["lon"] = str(wp.Location.Longitude)
                if wp.Location.Altitude is not None:
                    etree.SubElement(trkpt, "ele").text = str(wp.Location.Altitude)
                if wp.HR is not None or wp.Cadence is not None or wp.Temp is not None or wp.Calories is not None or wp.Power is not None:
                    exts = etree.SubElement(trkpt, "extensions")
                    gpxtpxexts = etree.SubElement(exts, GPXTPX + "TrackPointExtension")
                    if wp.HR is not None:
                        etree.SubElement(gpxtpxexts, GPXTPX + "hr").text = str(int(wp.HR))
                    if wp.Cadence is not None:
                        etree.SubElement(gpxtpxexts, GPXTPX + "cad").text = str(int(wp.Cadence))
                    if wp.Temp is not None:
                        etree.SubElement(gpxtpxexts, GPXTPX + "atemp").text = str(wp.Temp)

        return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("UTF-8")
