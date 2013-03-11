from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET
from tapiriik.services.service_authentication import ServiceAuthenticationType
from dropbox import client, rest, session
from django.core.urlresolvers import reverse


class DropboxService():
    ID = "dropbox"
    DisplayName = "Dropbox"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI

    def __init__(self):
        self.DBSess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "dropbox")
        self.DBCl = client.DropboxClient(self.DBSess)
        self.OutstandingReqTokens = {}

    def __getClient(self, serviceRec):
        sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "dropbox")
        sess.set_token(serviceRec["Authorization"]["Key"], serviceRec["Authorization"]["Secret"])
        return client.DropboxClient(sess)

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "dropbox"})
        pass

    def GenerateUserAuthorizationURL(self):
        reqToken = self.DBSess.obtain_request_token()
        self.OutstandingReqTokens[reqToken.key] = reqToken
        return self.DBSess.build_authorize_url(reqToken, oauth_callback=WEB_ROOT + reverse("oauth_return", kwargs={"service": "dropbox"}))

    def _getUserId(self, serviceRec):
        info = self.__getClient(serviceRec).account_info()
        return info['uid']

    def RetrieveAuthorizationToken(self, req):
        from tapiriik.services import Service
        tokenKey = req.GET["oauth_token"]
        token = self.OutstandingReqTokens[tokenKey]
        del self.OutstandingReqTokens[tokenKey]
        accessToken = self.DBSess.obtain_access_token(token)

        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Token": accessToken.key})
        if existingRecord is None:
            uid = self._getUserId({"Authorization": {"Key": accessToken.key, "Secret": accessToken.secret}})  # meh
        else:
            uid = existingRecord["ExternalID"]
        return (uid, {"Key": accessToken.key, "Secret": accessToken.secret})
