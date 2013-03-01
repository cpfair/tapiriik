from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.database import db
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import pytz
import re


class EndomondoService:
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword

    _activityMappings = {
        0:  ActivityType.Running,
        1:  ActivityType.Cycling,
        2:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        9:  ActivityType.Rowing,  # canoeing
        11: ActivityType.Rowing,
        14: ActivityType.Walking,  # fitness walking
        16: ActivityType.Hiking,
        17: ActivityType.Hiking,  # orienteering
        18: ActivityType.Walking,
        20: ActivityType.Swimming,
        22: ActivityType.Other,
        40: ActivityType.Swimming,  # scuba diving
        92: ActivityType.Wheelchair
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways
    SupportsCadence = False
    SupportsTemp = False
    SupportsPower = False

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
        print("response: " + resp.text + str(resp.status_code))
        if resp.text.strip() == "USER_UNKNOWN" or resp.text.strip() == "USER_EXISTS_PASSWORD_WRONG":
            return (None, None)  # maybe raise an exception instead?
        data = self._parseKVP(resp.text)
        return (data["userId"], {"AuthToken": data["authToken"], "SecureToken": data["secureToken"]})

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens endomondo distributes :\
        pass

    def _downloadRawTrackRecord(self, serviceRecord, trackId):
        params = {"authToken": serviceRecord["Authorization"]["AuthToken"], "trackId": trackId}
        response = requests.get("http://api.mobile.endomondo.com/mobile/readTrack", params=params)
        return response.text

    def _populateActivityFromTrackRecord(self, activity, recordText):
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
        rows = recordText.split("\n")
        for row in rows:
            if row == "OK" or len(row) == 0:
                continue
            split = row.split(";")
            if split[2] == "W":
                # init record
                activity.Distance = float(split[8]) * 1000
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
                wp.Timestamp = pytz.utc.localize(datetime.strptime(split[0], "%Y-%m-%d %H:%M:%S UTC"))  # it's like this as opposed to %z so I know when they change things (it'll break)
                if split[2] != "":
                    wp.Location = Location(float(split[2]), float(split[3]), None)
                    if split[6] != "":
                        wp.Location.Altitude = float(split[6])  # why this is missing: who knows?
                if split[7] != "":
                    wp.HR = float(split[7])
                activity.Waypoints.append(wp)

        activity.CalculateTZ()
        activity.AdjustTZ()

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        allItems = []

        params = {"authToken": serviceRecord["Authorization"]["AuthToken"], "maxResults": 45}

        while True:
            response = requests.get("http://api.mobile.endomondo.com/mobile/api/workout/list", params=params)
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIAuthorizationException("No authorization to retrieve activity list", serviceRecord)
                raise APIException("Unable to retrieve activity list " + str(response), serviceRecord)
            data = response.json()
            allItems += data["data"]
            if not exhaustive or data["more"] == False:
                break

        activities = []
        for act in allItems:
            if not act["has_points"]:
                continue  # it'll break strava, which needs waypoints to find TZ. Meh
            activity = UploadedActivity()
            activity.StartTime = pytz.utc.localize(datetime.strptime(act["start_time"], "%Y-%m-%d %H:%M:%S UTC"))
            activity.EndTime = activity.StartTime + timedelta(0, round(act["duration_sec"]))

            # attn service makers: why #(*%$ can't you all agree to use naive local time. So much simpler.
            cachedTrackData = db.endomondo_activity_cache.find_one({"TrackID": act["id"]})
            if cachedTrackData is None:
                cachedTrackData = {"TrackID": act["id"], "Data": self._downloadRawTrackRecord(serviceRecord, act["id"])}
                db.endomondo_activity_cache.insert(cachedTrackData)

            self._populateActivityFromTrackRecord(activity, cachedTrackData["Data"])

            if int(act["sport"]) in self._activityMappings:
                activity.Type = self._activityMappings[int(act["sport"])]

            activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["id"]}]
            activities.append(activity)
        return activities

    def DownloadActivity(self, serviceRecord, activity):
        pass  # the activity is fully populated at this point, thanks to meh API design decisions

    def UploadActivity(self, serviceRecord, activity):
        #http://api.mobile.endomondo.com/mobile/track?authToken=token&workoutId=2013-02-27%2020:51:45%20EST&sport=18&duration=0.08&calories=0.00&hydration=0.00&goalType=BASIC&goalType=DISTANCE&goalDistance=0.000000&deflate=true&audioMessage=true
        #...later...
        #http://api.mobile.endomondo.com/mobile/track?authToken=token&workoutId=2013-02-27%2020:51:45%20EST&sport=18&duration=23.04&calories=0.81&hydration=0.00&goalType=BASIC&goalType=DISTANCE&goalDistance=0.000000&deflate=true&audioMessage=false
        sportId = [k for k, v in self._activityMappings.items() if v == activity.Type]
        if len(sportId) == 0:
            raise ValueError("Endomondo service does not support activity type " + activity.Type)
        else:
            sportId = sportId[0]
        params = {"authToken": serviceRecord["Authorization"]["AuthToken"], "sport": sportId, "workoutId": "tap-sync-" + activity.UID}
        data = self._createUploadData(activity)

        response = requests.get("http://api.mobile.endomondo.com/mobile/track", params=params, data=data)
        if response.status_code != 200:
            raise APIException("Could not upload activity " + response.text)

    def _createUploadData(self, activity):
        if activity.StartTime.tzinfo is None:
            raise ValueError("Endomondo upload requires TZ info")

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
                WaypointType.Regular: ""
                }[wp.Type])

            if wp.Location is not None:
                line[2] = str(wp.Location.Latitude)
                line[3] = str(wp.Location.Longitude)
                if wp.Location.Altitude is not None:
                    line[6] = str(wp.Location.Altitude)

            if wp.HR is not None:
                line[7] = str(wp.HR)
            scsv.append(";".join(line))
        return "\n".join(scsv)