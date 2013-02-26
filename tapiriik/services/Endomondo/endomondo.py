from tapiriik.settings import WEB_ROOT, SITE_VER
from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.database import db
from tapiriik.services.interchange import UploadedActivity, ActivityType, Waypoint, WaypointType, Location
from tapiriik.services.api import APIException, APIAuthorizationException

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import urllib.parse
import json
import pytz
import re
import gzip
import base64

class EndomondoService:
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword

    SupportedActivities = [ActivityType.Running, ActivityType.Cycling]
    SupportsHR = True
    SupportsPower = True
    SupportsCalories = False  # don't think it does

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
        params = {"email": email, "password": password, "v": "2.4", "action": "pair", "deviceId": "TAP-SYNC", "country": "N/A"}  # note to future self: deviceId can't change otherwise we'll get different tokens back

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

        for row in recordText.split("\n"):
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

            activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["id"]}]
            activities.append(activity)
            print(activity)
        return activities

    def DownloadActivity(self, serviceRecord, activity):
        pass # the activity is fully populated at this point, thanks to meh API design decisions