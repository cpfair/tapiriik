# Synchronisation module for flow.polar.com
# (c) 2018 Anton Ashmarin, aashmarin@gmail.com
from tapiriik.settings import WEB_ROOT, POLAR_CLIENT_SECRET, POLAR_CLIENT_ID, POLAR_RATE_LIMITS
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.interchange import UploadedActivity, ActivityType
from tapiriik.services.tcx import TCXIO

from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth
from io import StringIO

import uuid
import gzip
import logging
import requests

logger = logging.getLogger(__name__)

class PolarFlowService(ServiceBase):
    ID = "polarflow"
    DisplayName = "Polar Flow"
    DisplayAbbreviation = "PF"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True # otherwise looks ugly in the small frame

    SupportsHR = SupportsCadence = SupportsPower = True

    ReceivesActivities = False # polar accesslink does not support polar data change.
    
    GlobalRateLimits = POLAR_RATE_LIMITS

    PartialSyncRequiresTrigger = True
    
    PartialSyncTriggerPollInterval = timedelta(minutes=1)

    # For mapping common->Polar Flow
    _activity_type_mappings = {
        ActivityType.Cycling: "Ride",
        ActivityType.MountainBiking: "Ride",
        ActivityType.Hiking: "Hike",
        ActivityType.Running: "Run",
        ActivityType.Walking: "Walk",
        ActivityType.Snowboarding: "Snowboard",
        ActivityType.Skating: "IceSkate",
        ActivityType.CrossCountrySkiing: "NordicSki",
        ActivityType.DownhillSkiing: "AlpineSki",
        ActivityType.Swimming: "Swim",
        ActivityType.Gym: "Workout",
        ActivityType.Rowing: "Rowing",
        ActivityType.Elliptical: "Elliptical",
        ActivityType.RollerSkiing: "RollerSki",
        ActivityType.StrengthTraining: "WeightTraining",
        ActivityType.Climbing: "RockClimbing",
    }

    SupportedActivities = list(_activity_type_mappings.keys())

    _api_endpoint = "https://www.polaraccesslink.com"

    def _register_user(self, access_token):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer {}".format(access_token)
        }
        res = requests.post(self._api_endpoint + "/v3/users",
            json={"member-id": uuid.uuid4().hex},
            headers=headers)
        return res.status_code == 200

    def _delete_user(self, serviceRecord):
        res = requests.delete(self._api_endpoint + "/v3/users/{userid}".format(userid=serviceRecord.ExternalID),
            headers=self._api_headers(serviceRecord))

    def _create_transaction(self, serviceRecord):
        res = requests.post(self._api_endpoint +
            "/v3/users/{userid}/exercise-transactions".format(userid=serviceRecord.ExternalID),
            headers=self._api_headers(serviceRecord))
        # No new training data status_code=204
        return res.json()["transaction-id"] if res.status_code == 201 else None

    def _commit_transaction(self, serviceRecord, id):
        res = requests.put(self._api_endpoint + 
            "/v3/users/{userid}/exercise-transactions/{transactionid}"
            .format(userid=serviceRecord.ExternalID, transactionid=id),
            headers=self._api_headers(serviceRecord))
        
        # todo : should handle responce code?
        # 200	OK	Transaction has been committed and data deleted	None
        # 204	No Content	No content when there is no data available	None
        # 404	Not Found	No transaction was found with given transaction id	None
        return True

    def _api_headers(self, serviceRecord):
        return {"Authorization": "Bearer {}".format(serviceRecord.Authorization["OAuthToken"])}

    def WebInit(self):
        params = {'response_type':'code',
                  'client_id': POLAR_CLIENT_ID,
                  'redirect_uri': WEB_ROOT + reverse("oauth_return", kwargs={"service": "polarflow"})}
        self.UserAuthorizationURL = "https://flow.polar.com/oauth2/authorization?" + urlencode(params)

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code",
                  "code": code,
                  "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "polarflow"})}

        response = requests.post("https://polarremote.com/v2/oauth2/token", data=params, auth=HTTPBasicAuth(POLAR_CLIENT_ID, POLAR_CLIENT_SECRET))
        data = response.json()

        if response.status_code != 200:
            raise APIException(data["error"])

        authorizationData = {"OAuthToken": data["access_token"]}
        userId = data["x_user_id"]

        try:
            self._register_user(data["access_token"])
        except requests.exceptions.HTTPError as err:
            # Error 409 Conflict means that the user has already been registered for this client.
            # That error can be ignored in this example.
            if err.response.status_code != 409:
                raise APIException("Unable to link user", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        return (userId, authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        self._delete_user(serviceRecord)

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        # There is no per-user webhook subscription with Polar Flow.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(True)

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        # As above.
        serviceRecord.SetPartialSyncTriggerSubscriptionState(False)

    def PollPartialSyncTrigger(self, multiple_index):
        response = requests.post(self._api_endpoint + "/v3/notifications", auth=HTTPBasicAuth(POLAR_CLIENT_ID, POLAR_CLIENT_SECRET))

        to_sync_ids = []
        if response.status_code == 200:
            for item in response.json()["available-user-data"]:
                if item["data-type"] == "EXERCISE":
                    to_sync_ids.append(item["user-id"])

        return to_sync_ids

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        activities = []
        exclusions = []
        
        transaction_id = self._create_transaction(serviceRecord)

        if transaction_id:
            res = requests.get(self._api_endpoint +
                "/v3/users/{userid}/exercise-transactions/{transactionid}"
                .format(userid=serviceRecord.ExternalID, transactionid=transaction_id),
                headers=self._api_headers(serviceRecord))
            
            if res.status_code == 200:
                for activity_link in res.json()["exercises"]:
                    activity = UploadedActivity()
                    #data = requests.get(activity_link, headers=self._api_headers(serviceRecord))
                    # tcx is gzipped
                    tcx_data_raw = requests.get(activity_link + "/tcx", headers=self._api_headers(serviceRecord))
                    tcx_data = gzip.GzipFile(fileobj=StringIO(tcx_data_raw)).read()
                    activity_ex = TCXIO.Parse(tcx_data.text.encode('utf-8'), activity)
                    activities.append(activity_ex)

            self._commit_transaction(serviceRecord, transaction_id)

        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        # Due to Polar Flow api specific (transactions + new-data-only)
        # it is easier to do all the download stuff in the DownloadActivityList function
        return activity

    def DeleteCachedData(self, serviceRecord):
        # Nothing to delete
        pass
