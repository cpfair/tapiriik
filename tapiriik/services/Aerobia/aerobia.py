from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity

import requests

class AerobiaService(ServiceBase):
    ID = "aerobia"
    DisplayName = "Aerobia"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword

    _urlRoot = "http://aerobia.ru/"
    _loginUrlRoot = _urlRoot + "users/sign_in"

    def Authorize(self, username, password):
        session = self._prepare_request()
        requestParameters = {"username": username,  "password": password}
        user_resp = session.get(self._loginUrlRoot, params=requestParameters)

        if user_resp.status_code != 200:
            raise APIException("Login error")

        response = user_resp.text()

        # Something extra unusual has happened
        raise APIException(
            "Invalid login - Unknown error",
            block=True,
            user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        # Sets the API key header necessary for all requests, and optionally the authentication token too.
    def _prepare_request(self, userToken=None):
        session = requests.Session()
        # If the serviceRecord was included, try to include the UserToken, authenticating the request
        # The service record will contain ExtendedAuthorization data if the user chose to remember login details.
        if userToken:
            session.headers.update(self._set_request_authentication_header(userToken))
        return session

        # Upon successful authentication by Authorize, the ExtendedAuthorization dict will have a UserToken
    def _set_request_authentication_header(self, userToken):
        return {"""authenticity_token""": userToken}