from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.database import cachedb, redis
from django.core.urlresolvers import reverse
from datetime import timedelta
import dropbox
import json
import logging
import pickle
import requests
logger = logging.getLogger(__name__)


class DropboxService(StorageServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    DisplayAbbreviation = "DB"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = True

    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format": "tcx", "Filename":"%Y-%m-%d_%H-%M-%S_#NAME_#TYPE"}

    def GetClient(self, serviceRec):
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
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": self.ID})

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

    def EnumerateFiles(self, svcRec, dbcl, root, cache):
        # Dropbox API v2 doesn't like / as root.
        if root == "/":
            root = ""
        # New Dropbox API prefers path_lower, it would seem.
        root = syncRoot.lower()

        try:
            list_result = dbcl.files_list_folder(syncRoot, recursive=True)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)

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
                    relPath = path.replace(root, "", 1)
                else:
                    relPath = path.replace("/Apps/tapiriik/", "", 1)  # dropbox api is meh api

                yield (path, relPath, path, file.rev)
            # Perform pagination.
            if list_result.has_more:
                list_result = dbcl.files_list_folder_continue(list_result.cursor)
            else:
                break

    def GetFileContents(self, serviceRecord, dbcl, path, storageid, cache):
        try:
            metadata, file = dbcl.files_download(path)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)

        return file.content, metadata.rev

    def PutFileContents(self, serviceRecord, dbcl, path, contents, cache):
        try:
            metadata = dbcl.files_upload(data.encode("UTF-8"), fpath, mode=dropbox.files.WriteMode.overwrite)
        except dropbox.exceptions.DropboxException as e:
            self._raiseDbException(e)

        return metadata.rev

    def MoveFile(self, serviceRecord, dbcl, path, destPath, cache):
        dbcl.file_move(path, path.replace(".tcx", ".tcx.summary-data"))

    def ServiceCacheDB(self):
        return cachedb.dropbox_cache

    def SyncRoot(self, svcRec):
        if not svcRec.Authorization["Full"]:
            syncRoot = "/"
        else:
            syncRoot = svcRec.Config["SyncRoot"]
        return syncRoot
