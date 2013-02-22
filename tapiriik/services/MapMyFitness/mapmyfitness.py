from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.services.api import APIException, APIAuthorizationException
from tapiriik.services.interchange import UploadedActivity, ActivityType, WaypointType, Waypoint, Location
from tapiriik.settings import WEB_ROOT, MAPMYFITNESS_CLIENT_KEY, MAPMYFITNESS_CLIENT_SECRET

from datetime import datetime, timedelta
import requests
from django.core.urlresolvers import reverse
from requests_oauthlib import OAuth1


class MapMyFitnessService():
    ID = "mapmyfitness"
    DisplayName = "MapMyFitness"
    AuthenticationType = ServiceAuthenticationType.OAuthSigned
    UserAuthorizationURL = None
    OutstandingOAuthRequestTokens = {}

    _activityMappings = {16: ActivityType.Running,
                         11: ActivityType.Cycling,
                         41: ActivityType.MountainBiking,
                         9: ActivityType.Walking,
                         24: ActivityType.Hiking,
                         398: ActivityType.DownhillSkiing,
                         397: ActivityType.CrossCountrySkiing,  # actually "backcountry" :S
                         107: ActivityType.Snowboarding,
                         86: ActivityType.Skating,  # ice skating
                         15: ActivityType.Swimming,
                         57: ActivityType.Rowing,  # canoe/rowing
                         211: ActivityType.Elliptical,
                         21: ActivityType.Other}
    SupportedActivities = list(_activityMappings.values())

    def WebInit(self):
        pass

    def GenerateUserAuthorizationURL(self):
        oauth = OAuth1(MAPMYFITNESS_CLIENT_KEY, client_secret=MAPMYFITNESS_CLIENT_SECRET)
        response = requests.post("http://api.mapmyfitness.com/3.1/oauth/request_token", auth=oauth)
        from urllib.parse import parse_qs, urlencode
        credentials = parse_qs(response.text)
        token = credentials["oauth_token"][0]
        self.OutstandingOAuthRequestTokens[token] = credentials["oauth_token_secret"][0]
        reqObj = {"oauth_token": token, "oauth_callback": WEB_ROOT + reverse("oauth_return", kwargs={"service": "mapmyfitness"})}
        return "http://api.mapmyfitness.com/3.1/oauth/authorize?" + urlencode(reqObj)

    def _getOauthClient(self, svcRec):
        return OAuth1(MAPMYFITNESS_CLIENT_KEY,
                       client_secret=MAPMYFITNESS_CLIENT_SECRET,
                       resource_owner_key=svcRec["Authorization"]["Key"],
                       resource_owner_secret=svcRec["Authorization"]["Secret"])

    def _getUserId(self, svcRec):
        oauth = self._getOauthClient(svcRec)
        response = requests.get("http://api.mapmyfitness.com/3.1/users/get_user", auth=oauth)
        responseData = response.json()
        return responseData["result"]["output"]["user"]["user_id"]

    def RetrieveAuthorizationToken(self, req):
        from tapiriik.services import Service

        token = req.GET.get("oauth_token")

        oauth = OAuth1(MAPMYFITNESS_CLIENT_KEY,
                       client_secret=MAPMYFITNESS_CLIENT_SECRET,
                       resource_owner_key=token,
                       resource_owner_secret=self.OutstandingOAuthRequestTokens[token])

        response = requests.post("http://api.mapmyfitness.com/3.1/oauth/access_token", auth=oauth)
        if response.status_code != 200:
            raise APIAuthorizationException("Invalid code", None)

        del self.OutstandingOAuthRequestTokens[token]

        from urllib.parse import parse_qs

        responseData = parse_qs(response.text)

        token = responseData["oauth_token"][0]
        secret = responseData["oauth_token_secret"][0]

        # hacky, but also totally their fault for not giving the user id in the token req
        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Key": token})
        if existingRecord is None:
            uid = self._getUserId({"Authorization": {"Key": token, "Secret": secret}})  # meh
        else:
            uid = existingRecord["ExternalID"]
        return (uid, {"Key": token, "Secret": secret})

    def RevokeAuthorization(self, serviceRecord):
        oauth = self._getOauthClient(serviceRecord)
        resp = requests.post("http://api.mapmyfitness.com/3.1/oauth/revoke", auth=oauth)
        if resp.status_code != 200:
            raise APIException("Unable to deauthorize MMF auth token, status " + str(resp.status_code) + " resp " + resp.text, serviceRecord)

    def _getActivityTypeHierarchy(self):
        if hasattr(self, "_activityTypes"):
            return self._activityTypes
        response = requests.get("http://api.mapmyfitness.com/3.1/workouts/get_activity_types")
        data = response.json()
        self._activityTypes = {}
        for actType in data["result"]["output"]["activity_types"]:
            self._activityTypes[int(actType["activity_type_id"])] = actType
        return self._activityTypes

    def _resolveActivityType(self, actType):
        self._getActivityTypeHierarchy()
        while actType not in self._activityMappings or self._activityTypes[actType]["parent_activity_type_id"] is not None:
            actType = int(self._activityTypes[actType]["parent_activity_type_id"])
        if actType in self._activityMappings:
            return self._activityMappings[actType]
        else:
            return ActivityType.Other

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        oauth = self._getOauthClient(serviceRecord)

        allItems = []

        offset = 0

        while True:
            response = requests.get("http://api.mapmyfitness.com/3.1/workouts/get_workouts?limit=25&start_record=" + str(offset), auth=oauth)
            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIAuthorizationException("No authorization to retrieve activity list", serviceRecord)
                raise APIException("Unable to retrieve activity list " + str(response), serviceRecord)
            data = response.json()
            allItems += data["result"]["output"]["workouts"]
            if not exhaustive or int(data["result"]["output"]["count"]) < 25:
                break

        activities = []
        for act in allItems:
            activity = UploadedActivity()
            activity.StartTime = datetime.strptime(act["workout_date"] + " " + act["workout_start_time"], "%Y-%m-%d %H:%M:%S")
            activity.EndTime = activity.StartTime + timedelta(0, round(float(act["time_taken"])))
            activity.Distance = act["distance"]

            activity.Type = self._resolveActivityType(int(act["activity_type_id"]))
            activity.CalculateUID()

            activity.UploadedTo = [{"Connection": serviceRecord, "ActivityID": act["workout_id"]}]
            activities.append(activity)
        return activities
