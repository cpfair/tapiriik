from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.sessioncache import SessionCache
from html.parser import HTMLParser

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

    _parser = HTMLParser()
    _sessionCache = SessionCache("aerobia", lifetime=timedelta(minutes=120), freshen_on_get=True)
    _userid = ""

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
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()
        # Without user-agent patch aerobia requests doesn't work   
        patch_requests_user_agent('Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko')

        requestParameters = {"user[email]": username,  "user[password]": password}
        res = session.post(self._loginUrlRoot, data=requestParameters)

        if res.status_code >= 500 and res.status_code < 600:
            raise APIException("Login exception %s - %s" % (res.status_code, res.text), user_exception=UserException(UserExceptionType.Authorization))

        id_match = re.search(r"users/(\d+)/workouts", res.text)
        if not id_match:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        else:
            self._userid = id_match.group(0)

        self._parser.feed(res.text)

        #TODO extract token
        session.access_token = "foo"
        self._sessionCache.Set(record.ExternalID if record else username, session)

        return session

    def _with_auth(self, session, params={}):
        # For whatever reason the access_token needs to be a GET parameter :(
        params.update({"""authenticity_token""": session.access_token})
        return params

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(username=username, password=password, skip_cache=True)

        return (self._userid, {}, {"Email": CredentialStore.Encrypt(username), "Password": CredentialStore.Encrypt(password)})

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass
