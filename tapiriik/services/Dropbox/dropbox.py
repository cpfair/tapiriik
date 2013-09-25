from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException, APIAuthorizationException, APIExcludeActivity, ServiceException
from tapiriik.services.interchange import ActivityType, UploadedActivity
from tapiriik.services.gpx import GPXIO
from tapiriik.services.tcx import TCXIO
from tapiriik.database import cachedb
from dropbox import client, rest, session
from django.core.urlresolvers import reverse
import re
import lxml
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

class DropboxService(ServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = True

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
    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format":"gpx", "Filename":"%Y-%m-%d_#NAME"}

    SupportsHR = SupportsCadence = True

    SupportedActivities = ActivityTaggingTable.keys()

    def __init__(self):
        self.OutstandingReqTokens = {}

    def _getClient(self, serviceRec):
        if serviceRec.Authorization["Full"]:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")
        sess.set_token(serviceRec.Authorization["Key"], serviceRec.Authorization["Secret"])
        return client.DropboxClient(sess)

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "dropbox"})
        pass

    def RequiresConfiguration(self, svcRec):
        return svcRec.Authorization["Full"] and ("SyncRoot" not in svcRec.Config or not len(svcRec.Config["SyncRoot"]))

    def GenerateUserAuthorizationURL(self, level=None):
        full = level == "full"
        if full:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")

        reqToken = sess.obtain_request_token()
        self.OutstandingReqTokens[reqToken.key] = reqToken
        return sess.build_authorize_url(reqToken, oauth_callback=WEB_ROOT + reverse("oauth_return", kwargs={"service": "dropbox", "level": "full" if full else "normal"}))

    def _getUserId(self, serviceRec):
        info = self._getClient(serviceRec).account_info()
        return info['uid']

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service
        tokenKey = req.GET["oauth_token"]
        token = self.OutstandingReqTokens[tokenKey]
        del self.OutstandingReqTokens[tokenKey]
        full = level == "full"
        if full:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")

        accessToken = sess.obtain_access_token(token)

        uid = int(req.GET["uid"])  # duh!
        return (uid, {"Key": accessToken.key, "Secret": accessToken.secret, "Full": full})

    def RevokeAuthorization(self, serviceRecord):
        pass  # :(

    def ConfigurationUpdating(self, svcRec, newConfig, oldConfig):
        from tapiriik.sync import Sync
        from tapiriik.auth import User
        if newConfig["SyncRoot"] != oldConfig["SyncRoot"]:
            Sync.ScheduleImmediateSync(User.AuthByService(svcRec), True)
            cachedb.dropbox_cache.update({"ExternalID": svcRec.ExternalID}, {"$unset": {"Structure": None}})

    def _raiseDbException(self, e):
        if e.status == 401:
                raise APIAuthorizationException("Authorization error - status " + str(e.status) + " reason " + str(e.error_msg) + " body " + str(e.body))
        raise APIException("API failure - status " + str(e.status) + " reason " + str(e.reason) + " body " + str(e.error_msg))

    def _folderRecurse(self, structCache, dbcl, path):
        hash = None
        existingRecord = [x for x in structCache if x["Path"] == path]
        children = [x for x in structCache if x["Path"].startswith(path) and x["Path"] != path]
        existingRecord = existingRecord[0] if len(existingRecord) else None
        if existingRecord:
            hash = existingRecord["Hash"]
        try:
            dirmetadata = dbcl.metadata(path, hash=hash)
        except rest.ErrorResponse as e:
            if e.status == 304:
                for child in children:
                    self._folderRecurse(structCache, dbcl, child["Path"])  # still need to recurse for children
                return  # nothing new to update here
            if e.status == 404:
                # dir doesn't exist any more, delete it and all children
                structCache[:] = (x for x in structCache if x != existingRecord and x not in children)
                return
            self._raiseDbException(e)
        if not existingRecord:
            existingRecord = {"Files": [], "Path": dirmetadata["path"]}
            structCache.append(existingRecord)

        existingRecord["Hash"] = dirmetadata["hash"]
        existingRecord["Files"] = []
        curDirs = []
        for file in dirmetadata["contents"]:
            if file["is_dir"]:
                curDirs.append(file["path"])
                self._folderRecurse(structCache, dbcl, file["path"])
            else:
                if not file["path"].lower().endswith(".gpx") and not file["path"].lower().endswith(".tcx"):
                    continue  # another kind of file
                existingRecord["Files"].append({"Rev": file["rev"], "Path": file["path"]})
        structCache[:] = (x for x in structCache if x["Path"] in curDirs or x not in children)  # delete ones that don't exist

    def _tagActivity(self, text):
        for act, pattern in self.ActivityTaggingTable.items():
            if re.search(pattern, text, re.IGNORECASE):
                return act
        return None

    def _getActivity(self, serviceRecord, dbcl, path):
        activityData = None

        try:
            f, metadata = dbcl.get_file_and_metadata(path)
        except rest.ErrorResponse as e:
            self._raiseDbException(e)

        cachedActivityRecord = cachedb.dropbox_activity_cache.find_one({"ExternalID":serviceRecord.ExternalID, "Path": path})
        if cachedActivityRecord:
            if cachedActivityRecord["Rev"] != metadata["rev"]:
                logger.debug("Outdated cache hit on %s" % path)
                cachedb.dropbox_activity_cache.remove(cachedActivityRecord)
            else:
                logger.debug("Cache hit on %s" % path)
                activityData = cachedActivityRecord["Data"]
                cachedb.dropbox_activity_cache.update(cachedActivityRecord, {"$set":{"Valid": datetime.utcnow()}})

        if not activityData:
            activityData = f.read()
            cachedb.dropbox_activity_cache.insert({"ExternalID": serviceRecord.ExternalID, "Rev": metadata["rev"], "Data": activityData, "Path": path, "Valid": datetime.utcnow()})


        try:
            if path.lower().endswith(".tcx"):
                act = TCXIO.Parse(activityData)
            else:
                act = GPXIO.Parse(activityData)
        except ValueError as e:
            raise APIExcludeActivity("Invalid GPX/TCX " + str(e), activityId=path)
        except lxml.etree.XMLSyntaxError as e:
            raise APIExcludeActivity("LXML parse error " + str(e), activityId=path)
        act.EnsureTZ()  # activity comes out of GPXIO with TZ=utc, this will recalculate it
        return act, metadata["rev"]

    def DownloadActivityList(self, svcRec, exhaustive=False):
        dbcl = self._getClient(svcRec)
        if not svcRec.Authorization["Full"]:
            syncRoot = "/"
        else:
            syncRoot = svcRec.Config["SyncRoot"]
        cache = cachedb.dropbox_cache.find_one({"ExternalID": svcRec.ExternalID})
        if cache is None:
            cache = {"ExternalID": svcRec.ExternalID, "Structure": [], "Activities": {}}
        if "Structure" not in cache:
            cache["Structure"] = []
        self._folderRecurse(cache["Structure"], dbcl, syncRoot)

        activities = []
        exclusions = []

        for dir in cache["Structure"]:
            for file in dir["Files"]:
                path = file["Path"]
                if svcRec.Authorization["Full"]:
                    relPath = path.replace(syncRoot, "", 1)
                else:
                    relPath = path.replace("/Apps/tapiriik/", "", 1)  # dropbox api is meh api

                existing = [(k, x) for k, x in cache["Activities"].items() if x["Path"] == relPath]  # path is relative to syncroot to reduce churn if they relocate it
                existing = existing[0] if existing else None
                if existing is not None:
                    existUID, existing = existing
                if existing and existing["Rev"] == file["Rev"]:
                    # don't need entire activity loaded here, just UID
                    act = UploadedActivity()
                    act.UID = existUID
                    act.StartTime = datetime.strptime(existing["StartTime"], "%H:%M:%S %d %m %Y %z")
                    if "EndTime" in existing:  # some cached activities may not have this, it is not essential
                        act.EndTime = datetime.strptime(existing["EndTime"], "%H:%M:%S %d %m %Y %z")
                else:
                    # get the full activity
                    try:
                        act, rev = self._getActivity(svcRec, dbcl, path)
                    except APIExcludeActivity as e:
                        logger.info("Encountered APIExcludeActivity %s" % str(e))
                        exclusions.append(e)
                        continue
                    del act.Waypoints
                    act.Waypoints = []  # Yeah, I'll process the activity twice, but at this point CPU time is more plentiful than RAM.
                    cache["Activities"][act.UID] = {"Rev": rev, "Path": relPath, "StartTime": act.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": act.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
                tagRes = self._tagActivity(relPath)
                act.UploadedTo = [{"Connection": svcRec, "Path": path, "Tagged":tagRes is not None}]

                act.Type = tagRes if tagRes is not None else ActivityType.Other

                logger.debug("Activity s/t %s" % act.StartTime)

                activities.append(act)

        cachedb.dropbox_cache.update({"ExternalID": svcRec.ExternalID}, cache, upsert=True)
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        # activity might not be populated at this point, still possible to bail out
        if not [x["Tagged"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]:
            if not (hasattr(serviceRecord, "Config") and "UploadUntagged" in serviceRecord.Config and serviceRecord.Config["UploadUntagged"]):
                raise ServiceException("Activity untagged", code="UNTAGGED")

        # activity might already be populated, if not download it again
        if len(activity.Waypoints) == 0:  # in the abscence of an actual Populated variable...
            path = [x["Path"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0]
            dbcl = self._getClient(serviceRecord)
            fullActivity, rev = self._getActivity(serviceRecord, dbcl, path)
            fullActivity.Type = activity.Type
            fullActivity.UploadedTo = activity.UploadedTo
            activity = fullActivity

        if len(activity.Waypoints) <= 1:
            raise APIExcludeActivity("Too few waypoints", activityId=[x["Path"] for x in activity.UploadedTo if x["Connection"] == serviceRecord][0])

        return activity

    def _clean_activity_name(self, name):
        # https://www.dropbox.com/help/145/en
        return re.sub("[><:\"|?*]", "", re.sub("[/\\\]", "-", name))

    def _format_file_name(self, format, activity):
        name = activity.StartTime.strftime(format)
        name = re.sub("#NAME", activity.Name if activity.Name and len(activity.Name) > 0 and activity.Name.lower() != activity.Type.lower() else "", name)
        name = re.sub("#TYPE", activity.Type, name)
        name = re.sub(r"([\W_])\1+", r"\1", name) # To handle cases where the activity is unnamed
        name = re.sub(r"^([\W_])|([\W_])$", "", name) # To deal with trailing-seperator weirdness (repeated seperator handled by prev regexp)
        return name

    def UploadActivity(self, serviceRecord, activity):
        activity.EnsureTZ()
        format = serviceRecord.GetConfiguration()["Format"]
        if format == "tcx":
            if "tcx" in activity.PrerenderedFormats:
                logger.debug("Using prerendered TCX")
                data = activity.PrerenderedFormats["tcx"]
            else:
                data = TCXIO.Dump(activity)
        else:
            if "gpx" in activity.PrerenderedFormats:
                logger.debug("Using prerendered GPX")
                data = activity.PrerenderedFormats["gpx"]
            else:
                data = GPXIO.Dump(activity)

        dbcl = self._getClient(serviceRecord)
        fname = self._format_file_name(serviceRecord.GetConfiguration()["Filename"], activity) + "." + format

        if not serviceRecord.Authorization["Full"]:
            fpath = "/" + fname
        else:
            fpath = serviceRecord.Config["SyncRoot"] + "/" + fname

        try:
            metadata = dbcl.put_file(fpath, data.encode("UTF-8"))
        except rest.ErrorResponse as e:
            self._raiseDbException(e)
        # fake this in so we don't immediately redownload the activity next time 'round
        cache = cachedb.dropbox_cache.find_one({"ExternalID": serviceRecord.ExternalID})
        cache["Activities"][activity.UID] = {"Rev": metadata["rev"], "Path": "/" + fname, "StartTime": activity.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": activity.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
        cachedb.dropbox_cache.update({"ExternalID": serviceRecord.ExternalID}, cache)  # not upsert, hope the record exists at this time...

    def DeleteCachedData(self, serviceRecord):
        cachedb.dropbox_cache.remove({"ExternalID": serviceRecord.ExternalID})
        cachedb.dropbox_activity_cache.remove({"ExternalID": serviceRecord.ExternalID})
