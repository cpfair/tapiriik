from datetime import timedelta
from .interchange import WaypointType

class ActivityStatisticCalculator:
    ImplicitPauseTime = timedelta(minutes=1, seconds=5)

    def CalculateDistance(act, startWpt=None, endWpt=None):
        import math
        dist = 0
        altHold = None  # seperate from the lastLoc variable, since we want to hold the altitude as long as required
        lastTimestamp = lastLoc = None

        flatWaypoints = act.GetFlatWaypoints()

        if not startWpt:
            startWpt = flatWaypoints[0]
        if not endWpt:
            endWpt = flatWaypoints[-1]

        for x in range(flatWaypoints.index(startWpt), flatWaypoints.index(endWpt) + 1):
            timeDelta = flatWaypoints[x].Timestamp - lastTimestamp if lastTimestamp else None
            lastTimestamp = flatWaypoints[x].Timestamp

            if flatWaypoints[x].Type == WaypointType.Pause or (timeDelta and timeDelta > ActivityStatisticCalculator.ImplicitPauseTime):
                lastLoc = None  # don't count distance while paused
                continue

            loc = flatWaypoints[x].Location
            if loc is None or loc.Longitude is None or loc.Latitude is None:
                # Used to throw an exception in this case, but the TCX schema allows for location-free waypoints, so we'll just patch over it.
                continue

            if loc and lastLoc:
                altHold = lastLoc.Altitude if lastLoc.Altitude is not None else altHold
                latRads = loc.Latitude * math.pi / 180
                meters_lat_degree = 1000 * 111.13292 + 1.175 * math.cos(4 * latRads) - 559.82 * math.cos(2 * latRads)
                meters_lon_degree = 1000 * 111.41284 * math.cos(latRads) - 93.5 * math.cos(3 * latRads)
                dx = (loc.Longitude - lastLoc.Longitude) * meters_lon_degree
                dy = (loc.Latitude - lastLoc.Latitude) * meters_lat_degree
                if loc.Altitude is not None and altHold is not None:  # incorporate the altitude when possible
                    dz = loc.Altitude - altHold
                else:
                    dz = 0
                dist += math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
            lastLoc = loc

        return dist

    def CalculateTimerTime(act, startWpt=None, endWpt=None):
        flatWaypoints = []
        for lap in act.Laps:
            flatWaypoints.append(lap.Waypoints)

        if len(flatWaypoints) < 3:
            # Either no waypoints, or one at the start and one at the end
            raise ValueError("Not enough waypoints to calculate timer time")
        duration = timedelta(0)
        if not startWpt:
            startWpt = flatWaypoints[0]
        if not endWpt:
            endWpt = flatWaypoints[-1]
        lastTimestamp = None
        for x in range(flatWaypoints.index(startWpt), flatWaypoints.index(endWpt) + 1):
            wpt = flatWaypoints[x]
            delta = wpt.Timestamp - lastTimestamp if lastTimestamp else None
            lastTimestamp = wpt.Timestamp
            if wpt.Type is WaypointType.Pause:
                lastTimestamp = None
            elif delta and delta > act.ImplicitPauseTime:
                delta = None  # Implicit pauses
            if delta:
                duration += delta
        if duration.total_seconds() == 0 and startWpt is None and endWpt is None:
            raise ValueError("Zero-duration activity")
        return duration

    def CalculateAverageMaxHR(act, startWpt=None, endWpt=None):
        flatWaypoints = act.GetFlatWaypoints()

        # Python can handle 600+ digit numbers, think it can handle this
        maxHR = 0
        cumulHR = 0
        samples = 0

        if not startWpt:
            startWpt = flatWaypoints[0]
        if not endWpt:
            endWpt = flatWaypoints[-1]

        for x in range(flatWaypoints.index(startWpt), flatWaypoints.index(endWpt) + 1):
            wpt = flatWaypoints[x]
            if wpt.HR:
                if wpt.HR > maxHR:
                    maxHR = wpt.HR
                cumulHR += wpt.HR
                samples += 1

        if not samples:
            return None, None

        cumulHR = cumulHR / samples
        return cumulHR, maxHR


