from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException, APIAuthorizationException
from tapiriik.services.interchange import ActivityType, UploadedActivity
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
        ActivityType.DownhillSkiing: "(downhill|down(hill)?\s*ski(ing)?)",
        ActivityType.CrossCountrySkiing: "(xc|cross.*country)\s*ski(ing)?",
        ActivityType.Snowboarding: "snowboard(ing)?",
        ActivityType.Skating: "skat(e|ing)?",
        ActivityType.Swimming: "swim",
        ActivityType.Wheelchair: "wheelchair",
        ActivityType.Rowing: "row",
        ActivityType.Elliptical: "elliptical",
        ActivityType.Other: "(other|unknown)"
    }

    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False}

    SupportsHR = SupportsCadence = True

    SupportedActivities = ActivityTaggingTable.keys()

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

    def ConfigurationUpdating(self, svcRec, newConfig, oldConfig):
        from tapiriik.auth import User
        if newConfig["SyncRoot"] != oldConfig["SyncRoot"]:
            User.ScheduleImmediateSync(User.AuthByService(svcRec), True)
            cachedb.dropbox_cache.update({"ExternalID": svcRec["ExternalID"]}, {"$unset": {"Structure": None}})

    def _folderRecurse(self, structCache, dbcl, path):
        hash = None
        existingRecord = [x for x in structCache if x["Path"] == path]
        existingRecord = existingRecord[0] if len(existingRecord) else None
        if existingRecord:
            hash = existingRecord["Hash"]
        try:
            dirmetadata = dbcl.metadata(path, hash=hash)
        except rest.ErrorResponse as e:
            if e.status == 304:
                return  # nothing new to update here
            raise  # an actual issue
        if not existingRecord:
            existingRecord = {"Files": [], "Path": dirmetadata["path"]}
            structCache.append(existingRecord)

        existingRecord["Hash"] = dirmetadata["hash"]
        existingRecord["Files"] = []
        for file in dirmetadata["contents"]:
            if file["is_dir"]:
                self._folderRecurse(structCache, dbcl, file["path"])
            else:
                if not file["path"].lower().endswith(".gpx"):
                    continue  # another kind of file
                existingRecord["Files"].append({"Rev": file["rev"], "Path": file["path"]})

    def _tagActivity(self, text):
        for act, pattern in self.ActivityTaggingTable.items():
            if re.search(pattern, text, re.IGNORECASE):
                return act
        return None

    def DownloadActivityList(self, svcRec, exhaustive=False):
        dbcl = self._getClient(svcRec)
        syncRoot = svcRec["Config"]["SyncRoot"]
        cache = cachedb.dropbox_cache.find_one({"ExternalID": svcRec["ExternalID"]})
        if cache is None:
            cache = {"ExternalID": svcRec["ExternalID"], "Structure": [], "Activities": {}}
        self._folderRecurse(cache["Structure"], dbcl, syncRoot)

        activities = []

        for dir in cache["Structure"]:
            for file in dir["Files"]:
                path = file["Path"]
                relPath = path.replace(syncRoot, "")
                existing = [(k, x) for k, x in cache["Activities"].items() if x["Path"] == relPath]  # path is relative to syncroot to reduce churn if they relocate it
                existing = existing[0] if existing else None
                if existing is not None:
                    existUID, existing = existing
                if existing and existing["Rev"] == file["Rev"]:
                    #  don't need entire activity loaded here, just UID
                    act = UploadedActivity()
                    act.UID = existUID
                else:
                    # get the activity and store the data locally
                    f, metadata = dbcl.get_file_and_metadata(path)
                    data = f.read()
                    act = GPXIO.Parse(data.decode("UTF-8"))
                    cache["Activities"][act.UID] = {"Rev": metadata["rev"], "Path": relPath}
                    cachedb.dropbox_data_cache.update({"UID": act.UID}, {"UID": act.UID, "Data": Binary(zlib.compress(data))}, upsert=True)  # easier than GridFS
                act.UploadedTo = [{"Connection": svcRec}]
                tagRes = self._tagActivity(relPath)
                act.Tagged = tagRes is not None
                act.Type = tagRes if tagRes is not None else ActivityType.Other
                activities.append(act)

        cachedb.dropbox_cache.update({"ExternalID": svcRec["ExternalID"]}, cache, upsert=True)
        return activities

    def DownloadActivity(self, serviceRecord, activity):
        # activity might not be populated at this point, still possible to bail out
        if not activity.Tagged:
            if "UploadUntagged" not in serviceRecord["Config"] or serviceRecord["Config"]["UploadUntagged"] is not True:
                raise APIException("Activity untagged", serviceRecord)

        # activity might already be populated, if not its data is in the local cache
        if len(activity.Waypoints) == 0:  # in the abscence of an actual Populated variable...
            data = zlib.decompress(cachedb.dropbox_data_cache.find_one({"UID": activity.UID})["Data"])  # really need compression?
            fullActivity = GPXIO.Parse(data.decode("UTF-8"))
            fullActivity.Tagged = activity.Tagged
            fullActivity.Type = activity.Type
            activity = fullActivity

        return activity

    def UploadActivity(self, serviceRecord, activity):
        activity.CalculateTZ()
        data = GPXIO.Dump(activity)
        cachedb.dropbox_data_cache.update({"UID": activity.UID}, {"UID": activity.UID, "Data": Binary(zlib.compress(data.encode("UTF-8")))}, upsert=True)
        dbcl = self._getClient(serviceRecord)
        fname = activity.Type + "_" + activity.StartTime.strftime("%d-%m-%Y") + ".gpx"
        fpath = serviceRecord["Config"]["SyncRoot"] + fname
        metadata = dbcl.put_file(fpath, data)

        #cache = cachedb.dropbox_cache.find_one({"ExternalID": svcRec["ExternalID"]}) FIXME
        #cache["Activities"][activity.UID] = {"Rev": metadata["rev"], "Path": fname}
        #cache["Structure"]["Files"][fpath] = {"Rev": metadata["rev"]}
        #cachedb.dropbox_cache.update({"ExternalID": svcRec["ExternalID"]}, cache)  # not upsert, hope the recort exists at this time...
        
