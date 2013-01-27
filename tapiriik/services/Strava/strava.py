from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_authentication import ServiceAuthenticationType
from django.core.urlresolvers import reverse
import httplib2
import urllib.parse
import json

class StravaService:
    ID = "strava"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "strava"})

    def Authorize(self, email, password):
        wc = httplib2.Http()
        # https://www.strava.com/api/v2/authentication/login
        params = {"email": email, "password": password}
        resp, data = wc.request("https://www.strava.com/api/v2/authentication/login", method="POST", body=urllib.parse.urlencode(params), headers={"Content-Type": "application/x-www-form-urlencoded"})
        if resp.status != 200:
            return None
        data = json.loads(data.decode('utf-8'))
        return (data["athlete"]["id"], {"Token": data["token"]})
