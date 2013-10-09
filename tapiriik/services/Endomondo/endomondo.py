from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import pytz
import re
import zlib
import os
import logging
import pickle

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    UserProfileURL = "http://www.endomondo.com/profile/{0}"
    UserActivityURL = "http://www.endomondo.com/workouts/{1}/{0}"

    _activityMappings = {
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,  # the order of these matters since it picks the first match for uploads
        1:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        9:  ActivityType.Rowing,  # canoeing
        18: ActivityType.Walking,
        14: ActivityType.Walking,  # fitness walking
        16: ActivityType.Hiking,
        17: ActivityType.Hiking,  # orienteering
        20: ActivityType.Swimming,
        40: ActivityType.Swimming,  # scuba diving
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    _reverseActivityMappings = {  # so that ambiguous events get mapped back to reasonable types
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        18: ActivityType.Walking,
        16: ActivityType.Hiking,
        20: ActivityType.Swimming,
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "endomondo"})

    def _parseKVP(self, data):
        out = {}
        for line in data.split("\n"):
            if line == "OK":
                continue
            match = re.match("(?P<key>[^=]+)=(?P<val>.+)$", line)
            if match is None:
                continue
            out[match.group("key")] = match.group("val")
        return out

    def Authorize(self, email, password):
        params = {"email": email, "password": password, "v": "2.4", "action": "pair", "deviceId": "TAP-SYNC-" + email.lower(), "country": "N/A"}  # note to future self: deviceId can't change intra-account otherwise we'll get different tokens back

        resp = requests.get("https://api.mobile.endomondo.com/mobile/auth", params=params)
        if resp.text.strip() == "USER_UNKNOWN" or resp.text.strip() == "USER_EXISTS_PASSWORD_WRONG":
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        data = self._parseKVP(resp.text)
        return (data["userId"], {"AuthToken": data["authToken"], "SecureToken": data["secureToken"]})

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens endomondo distributes :\
        pass

    def _downloadRawTrackRecord(self, serviceRecord, trackId):
        params = {"authToken": serviceRecord.Authorization["AuthToken"], "trackId": trackId}
        response = requests.get("http://api.mobile.endomondo.com/mobile/readTrack", params=params)
        return response.text

    def _populateActivityFromTrackData(self, activity, recordText, minimumWaypoints=False):
        activity.Waypoints = []
        ###       1ST RECORD      ###
        # userID;
        # timestamp - create date?;
        # type? W=1st
        # User name;
        # activity name;
        # activity type;
        # another timestamp - start time of event?;
        # duration.00;
        # distance (km);
        # kcal;
        #;
        # max alt;
        # min alt;
        # max HR;
        # avg HR;

        ###     TRACK RECORDS     ###
        # timestamp;
        # type (2=start, 3=end, 0=pause, 1=resume);
        # latitude;
        # longitude;
        #;
        #;
        # alt;
        # hr;
        wptsWithLocation = False
        wptsWithNonZeroAltitude = False
        rows = recordText.split("\n")
        for row in rows:
            if row == "OK" or len(row) == 0:
                continue
            split = row.split(";")
            if split[2] == "W":
                # init record
                activity.Distance = float(split[8]) * 1000 if split[8] != "" else None

                activity.Name = split[4]
            else:
                wp = Waypoint()
                if split[1] == "2":
                    wp.Type = WaypointType.Start
                elif split[1] == "3":
                    wp.Type = WaypointType.End
                elif split[1] == "0":
                    wp.Type = WaypointType.Pause
                elif split[1] == "1":
                    wp.Type = WaypointType.Resume
                else:
                    wp.Type == WaypointType.Regular

                if split[0] == "":
                    continue  # no timestamp, for whatever reason
                wp.Timestamp = pytz.utc.localize(datetime.strptime(split[0], "%Y-%m-%d %H:%M:%S UTC"))  # it's like this as opposed to %z so I know when they change things (it'll break)
                if split[2] != "":
                    wp.Location = Location(float(split[2]), float(split[3]), None)
                    if wp.Location.Longitude > 180 or wp.Location.Latitude > 90 or wp.Location.Longitude < -180 or wp.Location.Latitude < -90:
                        raise APIExcludeActivity("Out of range lat/lng")
                    if wp.Location.Latitude is not None and wp.Location.Latitude is not None:
                        wptsWithLocation = True
                    if split[6] != "":
                        wp.Location.Altitude = float(split[6])  # why this is missing: who knows?
                        if wp.Location.Altitude != 0:
                            wptsWithNonZeroAltitude = True

                if split[7] != "":
                    wp.HR = float(split[7])
                activity.Waypoints.append(wp)
                if wptsWithLocation and minimumWaypoints:
                    break
        activity.Waypoints = sorted(activity.Waypoints, key=lambda v: v.Timestamp)
        if wptsWithLocation:
            activity.EnsureTZ()
            if not wptsWithNonZeroAltitude:  # do this here so, should the activity run near sea level, altitude data won't be spotty
                for x in activity.Waypoints:  # clear waypoints of altitude data if all of them were logged at 0m (invalid)
                    if x.Location is not None:
                        x.Location.Altitude = None
        else:
            activity.Waypoints = []  # practically speaking

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        activities = []
        exclusions = []
        earliestDate = None
        earliestFirstPageDate = None
        paged = False

        while True:
            before = "" if earliestDate is None else earliestDate.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            params = {"authToken": serviceRecord.Authorization["AuthToken"], "maxResults": 45, "before": before}
            logger.debug("Req with " + str(params))
            response = requests.get("http://api.mobile.endomondo.com/mobile/api/workout/list", params=params)

            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve activity list " + str(response))
            data = response.json()

            if "error" in data and data["error"]["type"] == "AUTH_FAILED":
                raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            track_ids = []
            this_page_activities = []

            for act in data["data"]:
                startTime = pytz.utc.localize(datetime.strptime(act["start_time"], "%Y-%m-%d %H:%M:%S UTC"))
                if earliestDate is None or startTime < earliestDate:  # probably redundant, I would assume it works out the TZes...
                    earliestDate = startTime
                logger.debug("activity pre")
                if not act["has_points"]:
                    logger.warning("\t no pts")
                    exclusions.append(APIExcludeActivity("No points", activityId=act["id"]))
                    continue # it'll break strava, which needs waypoints to find TZ. Meh
                if "tracking" in act and act["tracking"]:
                    logger.warning("\t tracking")
                    exclusions.append(APIExcludeActivity("In progress", activityId=act["id"], permanent=False))
                    continue  # come back once they've completed the activity
                track_ids.append(act["id"])
                activity = UploadedActivity()
                activity.StartTime = startTime
                activity.EndTime = activity.StartTime + timedelta(0, round(act["duration_sec"]))
                logger.debug("\tActivity s/t " + str(activity.StartTime))

                if int(act["sport"]) in self._activityMappings:
                    activity.Type = self._activityMappings[int(act["sport"])]
                activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["id"]}]

                this_page_activities.append(activity)

            cached_track_tzs = cachedb.endomondo_activity_cache.find({"TrackID":{"$in": track_ids}})
            cached_track_tzs = dict([(x["TrackID"], x) for x in cached_track_tzs])

            for activity in this_page_activities:
                # attn service makers: why #(*%$ can't you all agree to use naive local time. So much simpler.
                cachedTrackData = None
                track_id = activity.UploadedTo[0]["ActivityID"]
                if track_id not in cached_track_tzs:
                    logger.debug("\t Resolving TZ for %s" % activity.StartTime)
                    cachedTrackData = self._downloadRawTrackRecord(serviceRecord, track_id)
                    self._populateActivityFromTrackData(activity, cachedTrackData, minimumWaypoints=True)
                    if not activity.TZ:
                        exclusions.append(APIExcludeActivity("Couldn't determine TZ", activityId=track_id))
                        continue
                    cachedTrackRecord = {"Owner": serviceRecord.ExternalID, "TrackID": track_id, "TZ": pickle.dumps(activity.TZ), "StartTime": activity.StartTime}
                    cachedb.endomondo_activity_cache.insert(cachedTrackRecord)
                else:
                    activity.TZ = pickle.loads(cached_track_tzs[track_id]["TZ"])
                    activity.AdjustTZ()  # Everything returned is in UTC
                activity.UploadedTo[0]["ActivityData"] = cachedTrackData
                activity.Waypoints = []
                activity.CalculateUID()
                activities.append(activity)

            if not paged:
                earliestFirstPageDate = earliestDate
            if not exhaustive or ("more" in data and data["more"] is False):
                break
            else:
                paged = True
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        uploadRecord = [x for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
        trackData = uploadRecord["ActivityData"]

        if not trackData:
            # If this is a new activity, we will already have the track data, otherwise download it.
            trackData = self._downloadRawTrackRecord(serviceRecord, uploadRecord["ActivityID"])

        self._populateActivityFromTrackData(activity, trackData)
        [x for x in activity.UploadedTo if x["Connection"] == serviceRecord][0].pop("ActivityData")
        if len(activity.Waypoints) <= 1:
            raise APIExcludeActivity("Too few waypoints", activityId=uploadRecord["ActivityID"])
        return activity

    def UploadActivity(self, serviceRecord, activity):
        #http://api.mobile.endomondo.com/mobile/track?authToken=token&workoutId=2013-02-27%2020:51:45%20EST&sport=18&duration=0.08&calories=0.00&hydration=0.00&goalType=BASIC&goalType=DISTANCE&goalDistance=0.000000&deflate=true&audioMessage=true
        #...later...
        #http://api.mobile.endomondo.com/mobile/track?authToken=token&workoutId=2013-02-27%2020:51:45%20EST&sport=18&duration=23.04&calories=0.81&hydration=0.00&goalType=BASIC&goalType=DISTANCE&goalDistance=0.000000&deflate=true&audioMessage=false
        sportId = [k for k, v in self._reverseActivityMappings.items() if v == activity.Type]
        if len(sportId) == 0:
            raise ValueError("Endomondo service does not support activity type " + activity.Type)
        else:
            sportId = sportId[0]
        params = {"authToken": serviceRecord.Authorization["AuthToken"], "sport": sportId, "workoutId": "tap-sync-" + str(os.getpid()) + "-" + activity.UID + "-" + activity.UploadedTo[0]["Connection"].Service.ID, "deflate": "true", "duration": activity.GetDuration().total_seconds(), "distance": activity.Distance / 1000 if activity.Distance is not None else None}
        data = self._createUploadData(activity)
        compressed_data = zlib.compress(data.encode("ASCII"))
        response = requests.get("http://api.mobile.endomondo.com/mobile/track", params=params, data=compressed_data)
        logger.debug("Upload result " + response.text)
        if response.status_code != 200:
            del compressed_data  # keep the error logs clean - automatically scrapes for local variables
            raise APIException("Could not upload activity " + response.text)

    def _createUploadData(self, activity):
        activity.EnsureTZ()

        #same format as they're downloaded afaik
        scsv = []
        for wp in activity.Waypoints:
            line = []
            for x in range(9):
                line.append("")

            line[0] = wp.Timestamp.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")  # who knows that's on the other end
            line[1] = ({
                WaypointType.Pause: "0",
                WaypointType.Resume: "1",
                WaypointType.Start: "2",
                WaypointType.End: "3",
                WaypointType.Regular: "",
                WaypointType.Lap: ""  # Endomondo has no lap tracking
                }[wp.Type])

            if wp.Location is not None:
                line[2] = str(wp.Location.Latitude) if wp.Location.Latitude is not None else ""
                line[3] = str(wp.Location.Longitude) if wp.Location.Longitude is not None else ""
                if wp.Location.Altitude is not None:
                    line[6] = str(wp.Location.Altitude)

            if wp.HR is not None:
                line[7] = str(wp.HR)
            scsv.append(";".join(line))
        return "\n".join(scsv)

    def DeleteCachedData(self, serviceRecord):
        cachedb.endomondo_activity_cache.remove({"Owner": serviceRecord.ExternalID})
