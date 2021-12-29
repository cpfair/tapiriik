from tapiriik.services.service_base import ServiceBase
from tapiriik.services.api import UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.interchange import ActivityType, UploadedActivity
from tapiriik.services.exception_tools import strip_context
from tapiriik.services.gpx import GPXIO
from tapiriik.services.tcx import TCXIO
import re
import lxml
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

class StorageServiceBase(ServiceBase):
    """
    A base class for all storage-like services (Dropbox, Google Drive, etc)
    """

    # Maximum path length that this service will accept.  Default is from Dropbox.
    MaxPathLen = 255

    ReceivesStationaryActivities = False

    ActivityTaggingTable = {  # earlier items have precedence over
        ActivityType.Running: "run(?!tastic)",
        ActivityType.MountainBiking: "m(oun)?t(ai)?n\s*bik(e|ing)",
        ActivityType.Cycling: "(cycl(e|ing)|bik(e|ing))",
        ActivityType.Walking: "walk",
        ActivityType.Hiking: "hik(e|ing)",
        ActivityType.DownhillSkiing: "(downhill|down(hill)?\s*ski(ing)?)",
        ActivityType.CrossCountrySkiing: "(xc|cross.*country)\s*ski(ing)?",
        ActivityType.Snowboarding:"snowboard(ing)?",
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

    SupportsHR = SupportsCadence = True

    SupportedActivities = ActivityTaggingTable.keys()

    def GetClient(self, svcRec):
        """ Return a client object for the service.  Will be passed back in to the various calls below """
        raise NotImplementedError()

    def GetFileContents(self, svcRec, client, path, storageid, cache):
        """ Return a tuple of (contents, version_number) for a given path. If this file was just enumerated,
+        storageid will be given (see EnumerateFiles below), otherwise it will be None. """
        raise NotImplementedError()

    def PutFileContents(self, svcRec, client, path, contents, cache):
        """ Write the contents to the file and return a version number for the newly written file. """
        raise NotImplementedError()

    def MoveFile(self, svcRec, client, path, destPath, cache):
        """ Move/rename the file "path" to "destPath". """
        raise NotImplementedError()

    def ServiceCacheDB(self):
        """ Get the cache DB object for this service, eg, cachedb.dropbox_cache """
        raise NotImplementedError()

    def SyncRoot(self, svcRec):
        """ Get the root directory on the service that we will be syncing to, eg, "/tapiriik/" """
        raise NotImplementedError()

    def EnumerateFiles(self, svcRec, client, root, cache):
        """ List the files available on the remote (applying some filtering,
        and using cache as appropriate.  Should yield tuples of:
          (fullPath, relPath, fileid)
        where storageid is some unique id that can be passed back to GetFileContents above.
        """
        raise NotImplementedError()

    def _tagActivity(self, text):
        for act, pattern in self.ActivityTaggingTable.items():
            if re.search(pattern, text, re.IGNORECASE):
                return act
        return None

    def _getActivity(self, serviceRecord, client, path, storageid, cache, base_activity=None):
        activityData, revision = self.GetFileContents(serviceRecord, client, path, storageid, cache)

        try:
            if path.lower().endswith(".tcx"):
                act = TCXIO.Parse(activityData, base_activity)
            else:
                act = GPXIO.Parse(activityData, base_activity)
        except ValueError as e:
            raise APIExcludeActivity("Invalid GPX/TCX " + str(e), activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))
        except lxml.etree.XMLSyntaxError as e:
            raise APIExcludeActivity("LXML parse error " + str(e), activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))
        return act, revision

    def _getCache(self, svcRec):
        cache = self.ServiceCacheDB().find_one({"ExternalID": svcRec.ExternalID}, {"ExternalID": True, "Activities": True})
        if cache is None:
            cache = {"ExternalID": svcRec.ExternalID, "Activities": {}}
        return cache

    def _storeCache(self, svcRec, cache):
        if "_id" in cache:
            self.ServiceCacheDB().save(cache)
        else:
            insert_result = self.ServiceCacheDB().insert(cache)
            cache["_id"] = insert_result.inserted_id

    def DownloadActivityList(self, svcRec, exhaustive=False):
        client = self.GetClient(svcRec)

        cache = self._getCache(svcRec)
        syncRoot = self.SyncRoot(svcRec)

        activities = []
        exclusions = []
        discovered_activity_cache_keys = set()

        for (path, relPath, storageid, revision) in self.EnumerateFiles(svcRec, client, syncRoot, cache):
            hashedRelPath = self._hash_path(relPath)
            discovered_activity_cache_keys.add(hashedRelPath)
            if hashedRelPath in cache["Activities"]:
                existing = cache["Activities"][hashedRelPath]
            else:
                existing = None

            if existing and existing["Rev"] == revision:
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
                    act, rev = self._getActivity(svcRec, client, path, storageid, cache)
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
                self._storeCache(svcRec, cache)
            tagRes = self._tagActivity(relPath)
            act.ServiceData = {"Path": path, "Tagged": tagRes is not None}

            act.Type = tagRes if tagRes is not None else ActivityType.Other

            logger.debug("Activity s/t %s" % act.StartTime)

            activities.append(act)

        # Drop deleted activities' records from cache.
        all_activity_cache_keys = set(cache["Activities"].keys())
        for deleted_key in all_activity_cache_keys - discovered_activity_cache_keys:
            del cache["Activities"][deleted_key]

        self._storeCache(svcRec, cache)

        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        # activity might not be populated at this point, still possible to bail out
        if not activity.ServiceData["Tagged"]:
            if not (hasattr(serviceRecord, "Config") and "UploadUntagged" in serviceRecord.Config and serviceRecord.Config["UploadUntagged"]):
                raise APIExcludeActivity("Activity untagged", permanent=False, activity_id=activity.ServiceData["Path"], user_exception=UserException(UserExceptionType.Untagged))

        path = activity.ServiceData["Path"]
        client = self.GetClient(serviceRecord)
        cache = self._getCache(serviceRecord)
        activity, rev = self._getActivity(serviceRecord, client, path, None, cache)
        self._storeCache(serviceRecord, cache)

        # Storage-based services don't support stationary activities yet.
        if activity.CountTotalWaypoints() <= 1:
            raise APIExcludeActivity("Too few waypoints", activity_id=path, user_exception=UserException(UserExceptionType.Corrupt))

        return activity

    def _hash_path(self, path):
        import hashlib
        # Can't use the raw file path as a dict key in Mongo, since who knows what'll be in it (periods especially)
        # Used the activity UID for the longest time, but that causes inefficiency when >1 file represents the same activity
        # So, this:
        csp = hashlib.new("md5")
        csp.update(path.encode("utf-8"))
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

        fname = self._format_file_name(serviceRecord.GetConfiguration()["Filename"], activity)[:self.MaxPathLen-5] + "." + format # max path length, and we have to save for the file ext (4) and the leading slash (1)

        client = self.GetClient(serviceRecord)

        syncRoot = self.SyncRoot(serviceRecord)
        if not syncRoot.endswith("/"):
            syncRoot += "/"
        fpath = syncRoot + fname

        cache = self._getCache(serviceRecord)
        revision = self.PutFileContents(serviceRecord, client, fpath, data.encode("UTF-8"), cache)

        # Fake this in so we don't immediately redownload the activity next time 'round
        cache["Activities"][self._hash_path("/" + fname)] = {"Rev": revision, "UID": activity.UID, "StartTime": activity.StartTime.strftime("%H:%M:%S %d %m %Y %z"), "EndTime": activity.EndTime.strftime("%H:%M:%S %d %m %Y %z")}
        self._storeCache(serviceRecord, cache)
        return fpath

    def DeleteCachedData(self, serviceRecord):
        self.ServiceCacheDB().remove({"ExternalID": serviceRecord.ExternalID})
