from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.sessioncache import SessionCache

import requests
import logging
import re

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AerobiaService(ServiceBase):
    ID = "aerobia"
    DisplayName = "Aerobia"
    DisplayAbbreviation = "ARB"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    _sessionCache = SessionCache("aerobia", lifetime=timedelta(minutes=120), freshen_on_get=True)

    _urlRoot = "http://aerobia.ru/"
    _loginUrlRoot = _urlRoot + "users/sign_in"

    def _get_session(self, record=None, username=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        from tapiriik.requests_lib import patch_requests_user_agent
        cached = self._sessionCache.Get(record.ExternalID if record else username)
        if cached and not skip_cache:
            logger.debug("Using cached credential")
            return cached
        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            username = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()
        # Without user-agent patch aerobia requests doesn't work   
        patch_requests_user_agent('Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko')

        requestParameters = {"user[email]": username,  "user[password]": password}
        res = session.post(self._loginUrlRoot, data=requestParameters)

        if res.status_code >= 500 and res.status_code < 600:
            raise APIException("Login exception %s - %s" % (res.status_code, res.text), user_exception=UserException(UserExceptionType.Authorization))

        # Userid is needed for urls
        idMatch = re.search(r"users/(\d+)/workouts", res.text)
        if not idMatch:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        else:
            session.user_id = idMatch.group(1)

        # Token is passed with GET queries as a parameter
        tokenMatch = re.search(r"meta content=\"(.+)\" name=\"csrf-token", res.text)
        if not tokenMatch:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        else:
            session.authenticity_token = tokenMatch.group(1)

        self._sessionCache.Set(record.ExternalID if record else username, session)

        return session

    def _with_auth(self, session, params={}):
        # For whatever reason the authenticity_token needs to be a GET parameter :(
        params.update({"\"authenticity_token\"": session.authenticity_token})
        return params

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(username=username, password=password, skip_cache=True)

        return (session.user_id, {}, {"Email": CredentialStore.Encrypt(username), "Password": CredentialStore.Encrypt(password)})

    def DownloadActivityList(self, serviceRecord, exhaustive_start_date=None):
        pass

    def DownloadActivity(self, serviceRecord, activity):
        pass

    # Should return an uploadId for storage and potential use in DeleteActivity
    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass
