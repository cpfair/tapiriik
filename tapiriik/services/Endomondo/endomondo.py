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

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        allItems = []


        params = {"authToken": serviceRecord["Authorization"]["AuthToken"], "compression": "gzip", "DEFLATE": True, "secureToken":serviceRecord["Authorization"]["SecureToken"], "maxResults": 45}

        # get TZ

        resp = requests.post("https://api.mobile.endomondo.com/mobile/api/profile/device/", params=params, data="")
        print(gzip.decompress(resp.content))
        return 

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
            activity.StartTime = datetime.strptime(act["start_time"], "%Y-%m-%d %H:%M:%S UTC")
            activity.EndTime = activity.StartTime + timedelta(0, round(act["duration_sec"]))
            #if act["type"] in self._activityMappings:
            #    activity.Type = self._activityMappings[act["type"]]

            activity.CalculateUID()
            activity.UploadedTo = [{"Connection":serviceRecord, "ActivityID":act["id"]}]
            activities.append(activity)
            print(activity)
        return activities