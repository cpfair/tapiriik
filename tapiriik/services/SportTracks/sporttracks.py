from tapiriik.settings import WEB_ROOT, SPORTTRACKS_OPENFIT_ENDPOINT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException, APIExcludeActivity

from django.core.urlresolvers import reverse
import pytz
from datetime import timedelta
import dateutil.parser
import requests
import json

import logging
logger = logging.getLogger(__name__)

class SportTracksService(ServiceBase):
    ID = "sporttracks"
    DisplayName = "SportTracks"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    OpenFitEndpoint = SPORTTRACKS_OPENFIT_ENDPOINT
    SupportsHR = True

    _activityMappings = {
        "running": ActivityType.Running,
        "cycling": ActivityType.Cycling,
        "walking": ActivityType.Walking,
        "hiking": ActivityType.Hiking,
        "skiing": ActivityType.CrossCountrySkiing,
        "skating": ActivityType.Skating,
        "swimming": ActivityType.Swimming,
        "rowing": ActivityType.Rowing,
        "other": ActivityType.Other
    }

    _reverseActivityMappings = {
        ActivityType.Running: "running",
        ActivityType.Cycling: "cycling",
        ActivityType.Walking: "walking",
        ActivityType.Hiking: "hiking",
        ActivityType.CrossCountrySkiing: "skiing",
        ActivityType.DownhillSkiing: "skiing",
        ActivityType.Skating: "skating",
        ActivityType.Swimming: "swimming",
        ActivityType.Rowing: "rowing",
        ActivityType.Other: "other"
    }

    SupportedActivities = list(_reverseActivityMappings.keys())

    def _get_cookies(self, email, password=None):
        return self._get_cookies_and_uid(email, password)[0]

    def _get_cookies_and_uid(self, email, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if password is None:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(email.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(email.ExtendedAuthorization["Email"])
        params = {"username": email, "password": password}
        resp = requests.post(self.OpenFitEndpoint + "/user/login", data=json.dumps(params), allow_redirects=False, headers={"Accept": "application/json", "Content-Type": "application/json"})
        if resp.status_code != 200:
            raise APIAuthorizationException("Invalid login")
        return resp.cookies, int(resp.json()["user"]["uid"])

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        cookies, uid = self._get_cookies_and_uid(email, password)
        return (uid, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        cookies = self._get_cookies(serviceRecord)
        activities = []
        exclusions = []
        pageUri = self.OpenFitEndpoint + "/fitnessActivities.json"
        while True:
            logger.debug("Req against " + pageUri)
            res = requests.get(pageUri, cookies=cookies)
            res = res.json()
            for act in res["items"]:
                activity = UploadedActivity()

                if len(act["name"].strip()):
                    activity.Name = act["name"]
                activity.StartTime = dateutil.parser.parse(act["start_time"])
                activity.TZ = pytz.FixedOffset(activity.StartTime.tzinfo._offset.total_seconds() / 60)  # Convert the dateutil lame timezones into pytz awesome timezones.
                activity.StartTime = activity.StartTime.replace(tzinfo=activity.TZ)
                activity.EndTime = activity.StartTime + timedelta(seconds=float(act["duration"]))

                logger.debug("Activity s/t " + str(activity.StartTime))
                activity.Distance = float(act["total_distance"])

                types = [x.strip().lower() for x in act["type"].split(":")]
                types.reverse()  # The incoming format is like "walking: hiking" and we want the most specific first
                activity.Type = None
                for type_key in types:
                    if type_key in self._activityMappings:
                        activity.Type = self._activityMappings[type_key]
                        break
                if not activity.Type:
                    raise APIException("Unknown activity type %s" % act["type"])

                activity.CalculateUID()
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityURI": act["uri"]}]
                activities.append(activity)
            if not exhaustive or "next" not in res or not len(res["next"]):
                break
            else:
                pageUri = res["next"]
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        activityURI = [x["ActivityURI"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        cookies = self._get_cookies(serviceRecord)
        activityData = requests.get(activityURI, cookies=cookies)
        activityData = activityData.json()
        if "location" not in activityData:
            raise APIExcludeActivity("No points")

        timerStops = []
        if "timer_stops" in activityData:
            for stop in activityData["timer_stops"]:
                timerStops.append([dateutil.parser.parse(stop[0]), dateutil.parser.parse(stop[1])])

        def isInTimerStop(timestamp):
            for stop in timerStops:
                if timestamp >= stop[0] and timestamp < stop[1]:
                    return True
                if timestamp >= stop[1]:
                    return False
            return False

        laps = []
        if "laps" in activityData:
            for lap in activityData["laps"]:
                laps.append(dateutil.parser.parse(lap["start_time"]))
        # Collate the individual streams into our waypoints.
        # Everything is resampled by nearest-neighbour to the rate of the location stream.
        parallel_indices = {}
        parallel_stream_lengths = {}
        for secondary_stream in ["elevation", "heartrate"]:
            if secondary_stream in activityData:
                parallel_indices[secondary_stream] = 0
                parallel_stream_lengths[secondary_stream] = len(activityData[secondary_stream])

        activity.Waypoints = []
        wasInPause = False
        currentLapIdx = 0
        for idx in range(0, len(activityData["location"]), 2):
            # Pick the nearest indices in the parallel streams
            for parallel_stream, parallel_index in parallel_indices.items():
                if parallel_index + 2 == parallel_stream_lengths[parallel_stream]:
                    continue  # We're at the end of this stream
                # Is the next datapoint a better choice than the current?
                if abs(activityData["location"][idx] - activityData[parallel_stream][parallel_index + 2]) < abs(activityData["location"][idx] - activityData[parallel_stream][parallel_index]):
                    parallel_indices[parallel_stream] += 2

            waypoint = Waypoint(activity.StartTime + timedelta(0, activityData["location"][idx]))
            waypoint.Location = Location(activityData["location"][idx+1][0], activityData["location"][idx+1][1], None)
            if "elevation" in parallel_indices:
                waypoint.Location.Altitude = activityData["elevation"][parallel_indices["elevation"]+1]

            if "heartrate" in parallel_indices:
                waypoint.HR = activityData["heartrate"][parallel_indices["heartrate"]+1]


            inPause = isInTimerStop(waypoint.Timestamp)
            waypoint.Type = WaypointType.Regular if not inPause else WaypointType.Pause
            if wasInPause and not inPause:
                waypoint.Type = WaypointType.Resume
            wasInPause = inPause

            # We only care if it's possible to start a new lap, i.e. there are more left
            if currentLapIdx + 1 < len(laps):
                if laps[currentLapIdx + 1] < waypoint.Timestamp:
                    # A new lap has started
                    waypoint.Type = WaypointType.Lap
                    currentLapIdx += 1

            activity.Waypoints.append(waypoint)

        activity.Waypoints[0].Type = WaypointType.Start
        activity.Waypoints[-1].Type = WaypointType.End
        return activity

    def UploadActivity(self, serviceRecord, activity):
        activityData = {}
        # Props to the SportTracks API people for seamlessly supprting activities with or without TZ data.
        activityData["start_time"] = activity.StartTime.isoformat()
        if activity.Name:
            activityData["name"] = activity.Name

        activityData["type"] = self._reverseActivityMappings[activity.Type]

        lap_starts = []
        timer_stops = []
        timer_stopped_at = None

        def stream_append(stream, wp, data):
            stream += [int((wp.Timestamp - activity.StartTime).total_seconds()), data]

        location_stream = []
        elevation_stream = []
        heartrate_stream = []
        for wp in activity.Waypoints:
            stream_append(location_stream, wp, [wp.Location.Latitude, wp.Location.Longitude])
            if wp.HR:
                stream_append(heartrate_stream, wp, wp.HR)
            if wp.Location.Altitude:
                stream_append(elevation_stream, wp, wp.Location.Altitude)
            if wp.Type == WaypointType.Lap:
                lap_starts.append(wp.Timestamp)
            if wp.Type == WaypointType.Pause and not timer_stopped_at:
                timer_stopped_at = wp.Timestamp
            if wp.Type != WaypointType.Pause and timer_stopped_at:
                timer_stops.append([timer_stopped_at, wp.Timestamp])
                timer_stopped_at = None

        activityData["elevation"] = elevation_stream
        activityData["heartrate"] = heartrate_stream
        activityData["location"] = location_stream
        activityData["laps"] = [x.isoformat() for x in lap_starts]
        activityData["timer_stops"] = [[y.isoformat() for y in x] for x in timer_stops]

        cookies = self._get_cookies(serviceRecord)
        upload_resp = requests.post(self.OpenFitEndpoint + "/fitnessActivities.json", data=json.dumps(activityData), cookies=cookies, headers={"Content-Type": "application/json"})
        if upload_resp.status_code != 200:
            raise APIException("Unable to upload activity %s" % upload_resp.text)


