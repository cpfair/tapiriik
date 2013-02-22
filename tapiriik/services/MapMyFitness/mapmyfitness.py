from tapiriik.services.service_authentication import ServiceAuthenticationType
from tapiriik.services.api import APIException, APIAuthorizationException
from tapiriik.settings import WEB_ROOT, MAPMYFITNESS_CLIENT_KEY, MAPMYFITNESS_CLIENT_SECRET

import requests
from django.core.urlresolvers import reverse
from requests_oauthlib import OAuth1


class MapMyFitnessService():
    ID = "mapmyfitness"
    DisplayName = "MapMyFitness"
    AuthenticationType = ServiceAuthenticationType.OAuthSigned
    UserAuthorizationURL = None
    OutstandingOAuthRequestTokens = {}

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
