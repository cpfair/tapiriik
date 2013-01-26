from tapiriik.settings import WEB_ROOT, RUNKEEPER_CLIENT_ID, RUNKEEPER_CLIENT_SECRET
from tapiriik.services.service_authentication import ServiceAuthenticationType
from django.core.urlresolvers import reverse
import httplib2
import urllib.parse
import json

class RunKeeperService():
    ID = "runkeeper"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserAuthorizationURL = None
    def WebInit(self):
        self.UserAuthorizationURL = "https://runkeeper.com/apps/authorize?client_id="+RUNKEEPER_CLIENT_ID+"&response_type=code&redirect_uri="+WEB_ROOT+reverse("oauth_return", kwargs={"service":"runkeeper"})

    def RetrieveAuthenticationToken(self, req):
        wc = httplib2.Http()
        #  might consider a real OAuth client 
        code = req.GET.get("code")
        params = {"grant_type":"authorization_code","code":code,"client_id":RUNKEEPER_CLIENT_ID,"client_secret":RUNKEEPER_CLIENT_SECRET,"redirect_uri":WEB_ROOT+reverse("oauth_return", kwargs={"service":"runkeeper"})}
        #return urllib.parse.urlencode(params)
        resp, data = wc.request("https://runkeeper.com/apps/token",method="POST",body=urllib.parse.urlencode(params), headers={"Content-Type":"application/x-www-form-urlencoded"})
        if resp.status != 200:
            raise ValueError("Invalid code")
        return {"AuthenticationToken": json.loads(data.decode('utf-8'))["access_token"]}
