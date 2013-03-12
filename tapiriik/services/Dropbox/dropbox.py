from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException, APIAuthorizationException
from tapiriik.services.interchange import ActivityType
from tapiriik.services.gpx import GPXIO
from tapiriik.database import cachedb
from dropbox import client, rest, session
from django.core.urlresolvers import reverse
from bson.binary import Binary
import zlib
import re


class DropboxService(ServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = RequiresConfiguration = True

    ActivityTaggingTable = {  # earlier items have precedence over
        ActivityType.Running: "run",
        ActivityType.MountainBiking: "m(oun)?t(ai)?n\s*bik(e|ing)",
        ActivityType.Cycling: "(cycl(e|ing)|bik(e|ing))",
        ActivityType.Walking: "walk",
        ActivityType.Hiking: "hik(e|ing)",
        ActivityType.DownhillSkiing: "down(hill)?\s*ski(ing)?",
        ActivityType.CrossCountrySkiing: "(xc|cross.*country)\s*ski(ing)?",
        ActivityType.Snowboarding: "snowboard(ing)?",
        ActivityType.Skating: "skat(e|ing)?",
        ActivityType.Swimming: "swim",
        ActivityType.Wheelchair: "wheelchair",
        ActivityType.Rowing: "row",
        ActivityType.Elliptical: "elliptical",
        ActivityType.Other: "(other|unknown)"
    }

    def __init__(self):
        self.DBSess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "dropbox")
        self.DBCl = client.DropboxClient(self.DBSess)
        self.OutstandingReqTokens = {}

    def _getClient(self, serviceRec):
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
        info = self._getClient(serviceRec).account_info()
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

    def _folderRecurse(self, structCache, dbcl, path):
        hash = None
        if path in structCache["Directories"]:
            hash = structCache["Directories"][path]["Hash"]
        try:
            dirmetadata = dbcl.metadata(path, hash=hash)
        except rest.ErrorResponse as e:
            if e.state == 304:
                return structCache  # nothing new to update here
            raise  # an actual issue

        structCache["Directories"][path] = {"Hash": dirmetadata["hash"]}
        for file in dirmetadata["contents"]:
            if file["is_dir"]:
                structCache = self._folderRecurse(structCache, dbcl, file["path"])
            else:
                if not file["path"].lower().endswith(".gpx"):
                    continue  # another kind of file
                structCache["Files"][file["path"]] = {"Rev": file["rev"]}
        return structCache

    def _tagActivity(self, text):
        for act, pattern in self.ActivityTaggingTable:
            if re.match(pattern, text, re.IGNORECASE):
                return act
        return None

    def DownloadActivityList(self, svcRec):
        dbcl = self._getClient(svcRec)
        syncRoot = svcRec["Config"]["SyncRoot"]
        cache = cachedb.dropbox_cache.find_one({"ExternalID": svcRec["ExternalID"]})
        if cache is None:
            cache = {"ExternalID": svcRec["ExternalID"], "Structure": {"Files": {}, "Directories": {}}, "Activities": {}}
        cache["Structure"] = self._folderRecurse(cache["Structure"], dbcl, syncRoot)

        activities = []

        for path, file in cache["Structure"]["Files"]:
            relPath = path.replace(syncRoot, "")
            existUID, existing = [(k, x) for k, x in cache["Activities"] if x["Path"] == relPath]  # path is relative to syncroot to reduce churn if they relocate it
            existing = existing[0] if existing else None
            if existing and existing["Rev"] == file["Rev"]:
                data = zlib.decompress(cachedb.dropbox_data_cache.find_one({"UID": existUID})["Data"])  # really need compression?
                act = GPXIO.Parse(data)
            else:
                # get the activity and store the data locally
                f, metadata = dbcl.get_file_and_metadata(path)
                data = f.read()
                act = GPXIO.Parse(data)
                cache["Activities"][act.UID] = {"Rev": metadata["rev"], "Path": relPath}
                cachedb.dropbox_data_cache.update({"UID": act.UID}, {"UID": act.UID, "Data": Binary(zlib.compress(data))}, upsert=True)  # easier than GridFS
            tagRes = self._tagActivity()
            act.Tagged = tagRes is not None
            act.Type = tagRes if tagRes is not None else ActivityType.Unknown
            activities.append(act)

        cachedb.dropbox_cache.update({"ExternalID": svcRec["ExternalID"]}, cache, upsert=True)
        return activities

    def DownloadActivity(self, serviceRecord, activity):
        # activity is already populated at this point, still need to check that it is tagged if preference requires it
        if not activity.Tagged:
            if "UploadUntagged" not in serviceRecord["Config"] or serviceRecord["Config"]["UploadUntagged"] is not True:
                raise APIException("Activity untagged", serviceRecord)
        return activity
