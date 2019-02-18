from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from tapiriik.database import cachedb
from tapiriik.services.api import APIException, ServiceExceptionScope, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from tapiriik.services.exception_tools import strip_context
from tapiriik.services.gpx import GPXIO
from tapiriik.services.interchange import ActivityType, UploadedActivity
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.tcx import TCXIO
from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET
import bson
import dropbox
import json
import logging
import lxml
import pickle
import re
import requests
logger = logging.getLogger(__name__)

class DropboxService(ServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    DisplayAbbreviation = "DB"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = True
    ReceivesStationaryActivities = False

    ActivityTaggingTable = {  # earlier items have precedence over
        ActivityType.Running: "run(?!tastic)",
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
        ActivityType.RollerSkiing: "rollerskiing",
        ActivityType.StrengthTraining: "strength( ?training)?",
        ActivityType.Gym: "(gym|workout)",
        ActivityType.Climbing: "climb(ing)?",
        ActivityType.StandUpPaddling: "(sup|stand( |-)/up ?paddl(e|ing))",
        ActivityType.Other: "(other|unknown)"
    }
    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format":"tcx", "Filename":"%Y-%m-%d_%H-%M-%S_#NAME_#TYPE"}

    SupportsHR = SupportsCadence = True

    SupportedActivities = ActivityTaggingTable.keys()

    def _app_credentials(self, full):
        if full:
            return (DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET)
        else:
            return (DROPBOX_APP_KEY, DROPBOX_APP_SECRET)

    def _getClient(self, serviceRec):
        from tapiriik.services import Service
        if "Secret" in serviceRec.Authorization:
            # Upgrade OAuth v1 token to v2.
            # The new Python SDK has a method for this
            # ...that requires initializing a client with a v2 user auth token :|
            upgrade_data = {
                "oauth1_token": serviceRec.Authorization["Key"],
                "oauth1_token_secret": serviceRec.Authorization["Secret"]
            }
            res = requests.post("https://api.dropboxapi.com/2/auth/token/from_oauth1",
                                json=upgrade_data,
                                auth=self._app_credentials(serviceRec.Authorization["Full"]))
            token = res.json()["oauth2_token"]
            # Update service record.
            Service.EnsureServiceRecordWithAuth(self, serviceRec.ExternalID, {
                "Token": token,
                "Full": serviceRec.Authorization["Full"]
            })
        else:
            token = serviceRec.Authorization["Token"]
        return dropbox.Dropbox(token)

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "dropbox"})

    def RequiresConfiguration(self, svcRec):
        return svcRec.Authorization["Full"] and ("SyncRoot" not in svcRec.Config or not len(svcRec.Config["SyncRoot"]))

    def _oauth2_flow(self, full, session):
        app_credentials = self._app_credentials(full)

        redirect_uri = WEB_ROOT + reverse("oauth_return",
                                          kwargs={"service": "dropbox", "level": "full" if full else "normal"})
        return dropbox.DropboxOAuth2Flow(
            app_credentials[0], app_credentials[1], redirect_uri, session,
            "dropbox-auth-csrf-token")

    def GenerateUserAuthorizationURL(self, session, level=None):
        return self._oauth2_flow(level == "full", session).start()

    def RetrieveAuthorizationToken(self, req, level):
        full = level == "full"
        result = self._oauth2_flow(full, req.session).finish(req.GET)
        uid = int(result.user_id)
        return (uid, {"Token": result.access_token, "Full": full})

    def RevokeAuthorization(self, serviceRecord):
        pass  # :(

    def ConfigurationUpdating(self, svcRec, newConfig, oldConfig):
        from tapiriik.sync import Sync
        from tapiriik.auth import User
        if newConfig["SyncRoot"] != oldConfig["SyncRoot"]:
            Sync.ScheduleImmediateSync(User.AuthByService(svcRec), True)
            cachedb.dropbox_cache.update({"ExternalID": svcRec.ExternalID}, {"$unset": {"Structure": None}})

    def _raiseDbException(self, e):
        if isinstance(e, dropbox.exceptions.AuthError):
            raise APIException("Authorization error - %s" % e, block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        if isinstance(e, dropbox.exceptions.ApiError) and \
           e.error.is_path() and \
           e.error.get_path().reason.is_insufficient_space():
            raise APIException("Dropbox quota error", block=True, user_exception=UserException(UserExceptionType.AccountFull, intervention_required=True))
        raise APIException("API failure - %s" % e)

    def _tagActivity(self, text):
        for act, pattern in self.ActivityTaggingTable.items():
            if re.search(pattern, text, re.IGNORECASE):
                return act
        return None

    def _getActivity(self, serviceRecord, dbcl, path, base_activity=None):
        try:
            metadata, file = dbcl.files_download(path)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)

        try:
            if path.lower().endswith(".tcx"):
                act = TCXIO.Parse(file.content, base_activity)
            else:
                act = GPXIO.Parse(file.content, base_activity)
        except ValueError as e:
            raise APIExcludeActivity("Invalid GPX/TCX " + str(e), activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))
        except lxml.etree.XMLSyntaxError as e:
            raise APIExcludeActivity("LXML parse error " + str(e), activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))
        return act, metadata.rev

    def DownloadActivityList(self, svcRec, exhaustive=False):
        dbcl = self._getClient(svcRec)
        if not svcRec.Authorization["Full"]:
            syncRoot = "/"
        else:
            syncRoot = svcRec.Config["SyncRoot"]
        # Dropbox API v2 doesn't like / as root.
        if syncRoot == "/":
            syncRoot = ""
        # New Dropbox API prefers path_lower, it would seem.
        syncRoot = syncRoot.lower()

        # There used to be a massive affair going on here to cache the folder structure locally.
        # Dropbox API 2.0 doesn't support the hashes I need for that.
        # Oh well. Throw that data out now. Well, don't load it at all.
        cache = cachedb.dropbox_cache.find_one({"ExternalID": svcRec.ExternalID}, {"ExternalID": True, "Activities": True})
        if cache is None:
            cache = {"ExternalID": svcRec.ExternalID, "Activities": {}}

        try:
            list_result = dbcl.files_list_folder(syncRoot, recursive=True)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)

        def cache_writeback():
            if "_id" in cache:
                cachedb.dropbox_cache.save(cache)
            else:
                insert_result = cachedb.dropbox_cache.insert(cache)
                cache["_id"] = insert_result.inserted_id


        activities = []
        exclusions = []
        discovered_activity_cache_keys = set()

        while True:
            for entry in list_result.entries:
                if not hasattr(entry, "rev"):
                    # Not a file -> we don't care.
                    continue
                path = entry.path_lower

                if not path.endswith(".gpx") and not path.endswith(".tcx"):
                    # Not an activity file -> we don't care.
                    continue

                if svcRec.Authorization["Full"]:
                    relPath = path.replace(syncRoot, "", 1)
                else:
                    relPath = path.replace("/Apps/tapiriik/", "", 1)  # dropbox api is meh api

                hashedRelPath = self._hash_path(relPath)
                discovered_activity_cache_keys.add(hashedRelPath)
                if hashedRelPath in cache["Activities"]:
                    existing = cache["Activities"][hashedRelPath]
                else:
                    existing = None

                if existing and existing["Rev"] == entry.rev:
                    # don't need entire activity loaded here, just UID
                    act = UploadedActivity()
                    act.UID = existing["UID"]
                    try:
                        act.StartTime = datetime.strptime(existing["StartTime"], "%H:%M:%S %d %m %Y %z")
                    except:
                        act.StartTime = datetime.strptime(existing["StartTime"], "%H:%M:%S %d %m %Y") # Exactly one user has managed to break %z :S
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

                    try:
                        act.EnsureTZ()
                    except:
                        pass # We tried.

                    act.Laps = []  # Yeah, I'll process the activity twice, but at this point CPU time is more plentiful than RAM.
                    cache["Activities"][hashedRelPath] = {"Rev": rev, "UID": act.UID, "StartTime": act.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": act.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
                    # Incrementally update the cache db.
                    # Otherwise, if we crash later on in listing
                    # (due to OOM or similar), we'll never make progress on this account.
                    cache_writeback()
                tagRes = self._tagActivity(relPath)
                act.ServiceData = {"Path": path, "Tagged": tagRes is not None}

                act.Type = tagRes if tagRes is not None else ActivityType.Other

                logger.debug("Activity s/t %s" % act.StartTime)

                activities.append(act)

            # Perform pagination.
            if list_result.has_more:
                list_result = dbcl.files_list_folder_continue(list_result.cursor)
            else:
                break

        # Drop deleted activities' records from cache.
        all_activity_cache_keys = set(cache["Activities"].keys())
        for deleted_key in all_activity_cache_keys - discovered_activity_cache_keys:
            del cache["Activities"][deleted_key]

        cache_writeback()
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        # activity might not be populated at this point, still possible to bail out
        if not activity.ServiceData["Tagged"]:
            if not (hasattr(serviceRecord, "Config") and "UploadUntagged" in serviceRecord.Config and serviceRecord.Config["UploadUntagged"]):
                raise APIExcludeActivity("Activity untagged", permanent=False, activity_id=activity.ServiceData["Path"], user_exception=UserException(UserExceptionType.Untagged))

        path = activity.ServiceData["Path"]
        dbcl = self._getClient(serviceRecord)
        activity, rev = self._getActivity(serviceRecord, dbcl, path, base_activity=activity)

        # Dropbox doesn't support stationary activities yet.
        if activity.CountTotalWaypoints() <= 1:
            raise APIExcludeActivity("Too few waypoints", activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))

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
        # Nothing outside BMP is allowed, either, apparently.
        return re.sub("[@><:\"|?*]|[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", "", re.sub("[/\\\]", "-", name))

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
            metadata = dbcl.files_upload(data.encode("UTF-8"), fpath, mode=dropbox.files.WriteMode.overwrite)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)
        # Fake this in so we don't immediately redownload the activity next time 'round
        cache = cachedb.dropbox_cache.find_one({"ExternalID": serviceRecord.ExternalID})
        cache["Activities"][self._hash_path("/" + fname)] = {"Rev": metadata.rev, "UID": activity.UID, "StartTime": activity.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": activity.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
        cachedb.dropbox_cache.update({"ExternalID": serviceRecord.ExternalID}, cache)  # not upsert, hope the record exists at this time...
        return fpath

    def DeleteCachedData(self, serviceRecord):
        cachedb.dropbox_cache.remove({"ExternalID": serviceRecord.ExternalID})
