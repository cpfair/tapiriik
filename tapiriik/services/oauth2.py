from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.services.sessioncache import SessionCache
from datetime import timedelta
import requests
import urllib.parse
import logging


logger = logging.getLogger(__name__)

class OAuth2Client():
    """
    A simple helper you can add to a service to automatically refresh oauth2 tokens
    """

    def __init__(self, clientID, clientSecret, tokenUrl, tokenTimeoutMin=60, cacheName=None):
        name = cacheName or clientID
        self._tokenCache = SessionCache(name, lifetime=timedelta(minutes=tokenTimeoutMin), freshen_on_get=False)
        self._tokenUrl = tokenUrl
        self._clientID = clientID
        self._clientSecret = clientSecret

    def _getAuthHeaders(self, serviceRec, token=None):
        token = token or self._tokenCache.Get(serviceRec.ExternalID)
        if not token:
            if not serviceRec.Authorization or "RefreshToken" not in serviceRec.Authorization:
                # When I convert the existing sportstracks users, people who didn't check the remember-credentials box will be stuck in limbo
                raise APIException("User not upgraded to OAuth", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            # Use refresh token to get access token (no redirect url required)
            params = {"grant_type": "refresh_token", "refresh_token": serviceRec.Authorization["RefreshToken"], "client_id": self._clientID, "client_secret": self._clientSecret}
            response = requests.post(self._tokenUrl, data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
            if response.status_code != 200:
                if response.status_code >= 400 and response.status_code < 500:
                    raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text))
            token = response.json()["access_token"]
            self._tokenCache.Set(serviceRec.ExternalID, token)

        return {"Authorization": "Bearer %s" % token}

    def revokeAuthorization(self, serviceRec, revokeUrl, token=None):
        # Implements most of the work for ServiceBase.RevokeAuthorization.
        token = token or self._tokenCache.Get(serviceRec.ExternalID)
        if not token:
            return
        resp = requests.post(revokeUrl, data={"token": token})
        if resp.status_code == 400:
            try:
                result = resp.json()
                if result.get("error") == "invalid_token":
                    logger.debug("Server said token %s invalid when we tried to revoke it, oh well.." % token)
                    # Token wasn't valid anyway, we're good
                    return
            except ValueError:
                raise APIException("Error revoking oauth2 token, status " + str(resp.status_code) + " resp " + resp.text)
        elif resp.status_code != 200:
            raise APIException("Unable to revoke oauth2 token, status " + str(resp.status_code) + " resp " + resp.text)

    def retrieveAuthorizationToken(self, service, req, redirectUri, getUidCallback):
        """
        Implements most of the work for ServiceBase.RetrieveAuthorizationToken.
        The getUidCallback is given the token data and must extract a usable
        user ID from it - or make requests to get one.
        """
        from tapiriik.services import Service
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": self._clientID, "client_secret": self._clientSecret, "redirect_uri": redirectUri}
        response = requests.post(self._tokenUrl, data=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if response.status_code != 200:
            print(response.text)
            raise APIException("Invalid code")
        data = response.json()
        access_token = data["access_token"]
        refresh_token = data["refresh_token"]

        existingRecord = Service.GetServiceRecordWithAuthDetails(service, {"Token": access_token})
        if existingRecord is None:
            uid = getUidCallback(data)
        else:
            uid = existingRecord.ExternalID

        return (uid, {"RefreshToken": refresh_token})

    def get(self, serviceRec, url, params=None, headers=None, access_token=None):
        auth_headers = self._getAuthHeaders(serviceRec, token=access_token)
        if headers:
            auth_headers.update(headers)
        return requests.get(url, params=params, headers=auth_headers)

    def put(self, serviceRec, url, params=None, data=None, headers=None, access_token=None):
        auth_headers = self._getAuthHeaders(serviceRec, token=access_token)
        if headers:
            auth_headers.update(headers)
        return requests.put(url, params=params, data=data, headers=auth_headers)

    def post(self, serviceRec, url, params=None, data=None, headers=None, access_token=None):
        auth_headers = self._getAuthHeaders(serviceRec, token=access_token)
        if headers:
            auth_headers.update(headers)
        return requests.post(url, params=params, data=data, headers=auth_headers)

    def patch(self, serviceRec, url, params=None, data=None, headers=None, access_token=None):
        auth_headers = self._getAuthHeaders(serviceRec, token=access_token)
        if headers:
            auth_headers.update(headers)
        return requests.patch(url, params=params, data=data, headers=auth_headers)

    def session(self, serviceRec):
        s = requests.Session()
        s.headers.update(self._getAuthHeaders(serviceRec))
        return s
