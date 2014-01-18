from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.api import APIException, ServiceExceptionScope, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from tapiriik.services.interchange import ActivityType, UploadedActivity
from tapiriik.services.exception_tools import strip_context
from tapiriik.services.gpx import GPXIO
from tapiriik.services.tcx import TCXIO
from tapiriik.database import cachedb
from dropbox import client, rest, session
from django.core.urlresolvers import reverse
import re
import lxml
from datetime import datetime
import logging
import bson
logger = logging.getLogger(__name__)

class DropboxService(ServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = True
    ReceivesStationaryActivities = False

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
    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format":"tcx", "Filename":"%Y-%m-%d_#NAME_#TYPE"}

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
            raise APIException("Authorization error - status " + str(e.status) + " reason " + str(e.error_msg) + " body " + str(e.body), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        if e.status == 507:
            raise APIException("Dropbox quota error", block=True, user_exception=UserException(UserExceptionType.AccountFull, intervention_required=True))
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

        if not activityData:
            activityData = f.read()


        try:
            if path.lower().endswith(".tcx"):
                act = TCXIO.Parse(activityData)
            else:
                act = GPXIO.Parse(activityData)
        except ValueError as e:
            raise APIExcludeActivity("Invalid GPX/TCX " + str(e), activityId=path)
        except lxml.etree.XMLSyntaxError as e:
            raise APIExcludeActivity("LXML parse error " + str(e), activityId=path)
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

                hashedRelPath = self._hash_path(relPath)
                if hashedRelPath in cache["Activities"]:
                    existing = cache["Activities"][hashedRelPath]
                else:
                    existing = None

                if not existing:
                    # Continue to use the old records keyed by UID where possible
                    existing = [(k, x) for k, x in cache["Activities"].items() if "Path" in x and x["Path"] == relPath]  # path is relative to syncroot to reduce churn if they relocate it
                    existing = existing[0] if existing else None
                    if existing is not None:
                        existUID, existing = existing
                        existing["UID"] = existUID

                if existing and existing["Rev"] == file["Rev"]:
                    # don't need entire activity loaded here, just UID
                    act = UploadedActivity()
                    act.UID = existing["UID"]
                    act.StartTime = datetime.strptime(existing["StartTime"], "%H:%M:%S %d %m %Y %z")
                    if "EndTime" in existing:  # some cached activities may not have this, it is not essential
                        act.EndTime = datetime.strptime(existing["EndTime"], "%H:%M:%S %d %m %Y %z")
                else:
                    logger.debug("Retrieving %s (%s)" % (path, "outdated meta cache" if existing else "not in meta cache"))
                    # get the full activity
                    try:
                        act, rev = self._getActivity(svcRec, dbcl, path)
                    except APIExcludeActivity as e:
                        logger.info("Encountered APIExcludeActivity %s" % str(e))
                        exclusions.append(strip_context(e))
                        continue
                    if hasattr(act, "OriginatedFromTapiriik") and not act.CountTotalWaypoints():
                        # This is one of the files created when TCX export was hopelessly broken for non-GPS activities.
                        # Right now, no activities in dropbox from tapiriik should be devoid of waypoints - since dropbox doesn't receive stationary activities
                        # In the future when this changes, will obviously have to modify this code to also look at modification dates or similar.
                        if ".tcx.summary-data" in path:
                            logger.info("...summary file already moved")
                        else:
                            logger.info("...moving summary-only file")
                            dbcl.file_move(path, path.replace(".tcx", ".tcx.summary-data"))
                        continue # DON'T include in listing - it'll be regenerated
                    del act.Laps
                    act.Laps = []  # Yeah, I'll process the activity twice, but at this point CPU time is more plentiful than RAM.
                    cache["Activities"][hashedRelPath] = {"Rev": rev, "UID": act.UID, "StartTime": act.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": act.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
                tagRes = self._tagActivity(relPath)
                act.ServiceData = {"Path": path, "Tagged":tagRes is not None}

                act.Type = tagRes if tagRes is not None else ActivityType.Other

                logger.debug("Activity s/t %s" % act.StartTime)

                activities.append(act)

        if "_id" in cache:
            cachedb.dropbox_cache.save(cache)
        else:
            cachedb.dropbox_cache.insert(cache)
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        # activity might not be populated at this point, still possible to bail out
        if not activity.ServiceData["Tagged"]:
            if not (hasattr(serviceRecord, "Config") and "UploadUntagged" in serviceRecord.Config and serviceRecord.Config["UploadUntagged"]):
                raise APIExcludeActivity("Activity untagged", permanent=False, activityId=activity.ServiceData["Path"])

        # activity might already be populated, if not download it again
        path = activity.ServiceData["Path"]
        dbcl = self._getClient(serviceRecord)
        fullActivity, rev = self._getActivity(serviceRecord, dbcl, path)
        fullActivity.Type = activity.Type
        fullActivity.ServiceDataCollection = activity.ServiceDataCollection
        activity = fullActivity

        # Dropbox doesn't support stationary activities yet.
        if activity.CountTotalWaypoints() <= 1:
            raise APIExcludeActivity("Too few waypoints", activityId=path)

        return activity

    def _hash_path(self, path):
        import hashlib
        # Can't use the raw file path as a dict key in Mongo, since who knows what'll be in it (periods especially)
        # Used the activity UID for the longest time, but that causes inefficiency when >1 file represents the same activity
        # So, this:
        csp = hashlib.new("md5")
        csp.update(path.encode('utf-8'))
        return csp.hexdigest()

    def _clean_activity_name(self, name):
        # https://www.dropbox.com/help/145/en
        return re.sub("[><:\"|?*]", "", re.sub("[/\\\]", "-", name))

    def _format_file_name(self, format, activity):
        name_pattern = re.compile("#NAME", re.IGNORECASE)
        type_pattern = re.compile("#TYPE", re.IGNORECASE)
        name = activity.StartTime.strftime(format)
        name = name_pattern.sub(self._clean_activity_name(activity.Name) if activity.Name and len(activity.Name) > 0 and activity.Name.lower() != activity.Type.lower() else "", name)
        name = type_pattern.sub(activity.Type, name)
        name = re.sub(r"([\W_])\1+", r"\1", name) # To handle cases where the activity is unnamed
        name = re.sub(r"^([\W_])|([\W_])$", "", name) # To deal with trailing-seperator weirdness (repeated seperator handled by prev regexp)
        return name

    def UploadActivity(self, serviceRecord, activity):
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
        fname = self._format_file_name(serviceRecord.GetConfiguration()["Filename"], activity)[:250] + "." + format # DB has a max path component length of 255 chars, and we have to save for the file ext (4) and the leading slash (1)

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
        cache["Activities"][self._hash_path("/" + fname)] = {"Rev": metadata["rev"], "UID": activity.UID, "StartTime": activity.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": activity.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
        cachedb.dropbox_cache.update({"ExternalID": serviceRecord.ExternalID}, cache)  # not upsert, hope the record exists at this time...
        return fpath

    def DeleteCachedData(self, serviceRecord):
        cachedb.dropbox_cache.remove({"ExternalID": serviceRecord.ExternalID})
