# Synchronisation module for flow.polar.com
# (c) 2018 Anton Ashmarin, aashmarin@gmail.com
from tapiriik.settings import WEB_ROOT, POLAR_CLIENT_SECRET, POLAR_CLIENT_ID, POLAR_RATE_LIMITS
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException

from django.core.urlresolvers import reverse
from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth

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

    ReceivesActivities = False # polar accesslink does not support polar data chenge.
    
    GlobalRateLimits = POLAR_RATE_LIMITS

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

        return (userId, authorizationData)

    def RevokeAuthorization(self, serviceRecord):
        # cannot revoke
        pass