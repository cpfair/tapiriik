from tapiriik.settings import WEB_ROOT, GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from tapiriik.database import cachedb, redis
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from googleapiclient import errors
from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
from django.core.urlresolvers import reverse
import logging
import httplib2
import requests
import json

logger = logging.getLogger(__name__)

GOOGLE_REVOKE_URI = 'https://accounts.google.com/o/oauth2/revoke'

# Full scope needed so that we can read files that user adds by hand
_OAUTH_SCOPE = "https://www.googleapis.com/auth/drive"

# Mimetypes to use when uploading, keyed by extension
_MIMETYPES = {
    "gpx": "application/gpx+xml",
    "tcx": "application/vnd.garmin.tcx+xml"
}

# Mimetype given to folders on google drive.
_FOLDER_MIMETYPE = "application/vnd.google-apps.folder"

def _basename(path):
    return path.split("/")[-1]

class GoogleDriveService(StorageServiceBase):
    ID = "googledrive"
    DisplayName = "Google Drive"
    DisplayAbbreviation = "GD"
    AuthenticationType = ServiceAuthenticationType.OAuth
    Configurable = True
    ReceivesStationaryActivities = False
    AuthenticationNoFrame = True
    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format":"tcx", "Filename":"%Y-%m-%d_#NAME_#TYPE"}

    def _oauthFlow(self):
        return_url = WEB_ROOT + reverse("oauth_return", kwargs={"service": self.ID})
        flow = OAuth2WebServerFlow(GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET, _OAUTH_SCOPE,
                                   redirect_uri=return_url, access_type='offline')
        return flow

    def GetClient(self, serviceRec):
        credentials = OAuth2Credentials.from_json(serviceRec.Authorization["Credentials"])
        http = httplib2.Http()
        if credentials.access_token_expired:
            logger.debug("Refreshing Google Drive credentials")
            credentials.refresh(http)
            serviceRec.Authorization["Credentials"] = credentials.to_json()
            # Note: refreshed token doesn't get persisted, but will stick
            # around in the serviceRec for the duration of a sync.
            # TODO: Should use a SessionCache - tokens last 60 mins by default
        http = credentials.authorize(http)
        drive_service = build("drive", "v2", http=http)
        return drive_service

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("oauth_redirect", kwargs={"service": self.ID})
        pass

    def GenerateUserAuthorizationURL(self, session, level=None):
        flow = self._oauthFlow()
        return flow.step1_get_authorize_url()

    def _getUserId(self, svcRec):
        client = self.GetClient(svcRec)
        try:
            about = client.about().get().execute()
            # TODO: Is this a good user ID to use?  Could also use email..
            return about["rootFolderId"]
        except errors.HttpError as error:
            raise APIException("Google drive error fetching user ID - %s" % error)

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service
        flow = self._oauthFlow()
        code = req.GET["code"]
        credentials = flow.step2_exchange(code)
        cred_json = credentials.to_json()

        uid = self._getUserId(ServiceRecord({"Authorization": {"Credentials": cred_json}}))
        return (uid, {"Credentials": cred_json})

    def RevokeAuthorization(self, serviceRec):
        credentials = OAuth2Credentials.from_json(serviceRec.Authorization["Credentials"])
        # should this just be calling credentials.revoke()?
        resp = requests.post(GOOGLE_REVOKE_URI, data={"token": credentials.access_token})
        if resp.status_code == 400:
            try:
                result = json.loads(resp.text)
                if result.get("error") == "invalid_token":
                    logger.debug("Google drive said token %s invalid when we tried to revoke it, oh well.." % credentials.access_token)
                    # Token wasn't valid anyway, we're good
                    return
            except ValueError:
                raise APIException("Error revoking Google Drive auth token, status " + str(resp.status_code) + " resp " + resp.text)
        elif resp.status_code != 200:
            raise APIException("Unable to revoke Google Drive auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _idCache(self, cache):
        if "FileIDs" not in cache:
            cache["FileIDs"] = []
        return cache["FileIDs"]

    def _getFileId(self, client, path, cache):
        """ get file id for the given path.  Returns None if the path does not exist.
        also returns cache hits used in determining the id, in case it turns out to be wrong.
        """
        id_cache = self._idCache(cache)

        if path == "":
            path = "/"

        assert(path.startswith("/"))
        if path.endswith("/"):
            path = path[:-1]
        currentid = "root"
        parts = path.split("/")
        offset = 1
        cachehits = set()

        while offset < len(parts):
            existingRecord = [x for x in id_cache if (x["Parent"] == currentid and x["Name"] == parts[offset])]
            if len(existingRecord):
                existingRecord = existingRecord[0]
                currentid = existingRecord["ID"]
                cachehits.add(currentid)
            else:
                try:
                    params = {"q": "title = '%s'" % parts[offset], "fields": "items/id"}
                    children = client.children().list(folderId=currentid, **params).execute()
                except errors.HttpError as error:
                    raise APIException("Error listing Google Drive contents - %s" + str(error))

                if not len(children.get("items", [])):
                    if cachehits:
                        # The cache may have led us astray - clear hits and try again
                        self._removeCachedIds(cachehits, cache)
                        return self._getFileId(client, path, cache)
                    else:
                        return None, None
                childid = children["items"][0]["id"]
                id_cache.append({"ID": childid, "Parent": currentid, "Name": parts[offset]})
                currentid = childid
            offset += 1
        return currentid, cachehits

    def _removeCachedIds(self, fileids, cache):
        id_cache = self._idCache(cache)
        id_cache[:] = (x for x in id_cache if x["ID"] not in fileids)

    def _getFile(self, client, path, storageid, cache):
        logger.info("getfile %s %s" % (storageid, path))
        if storageid:
            file_id = storageid
            cachehits = None
        else:
            file_id, cachehits = self._getFileId(client, path, cache)
            logger.info("Trying to fetch id %s from path %s" % (file_id, path))
            if not file_id:
                return None  # File not found.

        try:
            file = client.files().get(fileId=file_id).execute()
        except errors.HttpError as error:
            if error.resp.status == 404 and cachehits:
                logger.debug("Google drive cache %s invalid - 404" % file_id)
                # remove cache entries and try again
                self._removeCachedIds(cachehits, cache)
                return self._getFile(client, path, storageid, cache)
            raise APIException("Error %d fetching Google Drive file URL - %s" % (error.resp.status, str(error)))

        if file.get("title") != _basename(path):
            if not cachehits:
                # shouldn't happen?
                raise APIException("Error fetching Google Drive file - name didn't match")

            # Cached file ID now has different name - invalidate and try again
            logger.debug("Google drive cache %s invalid - name no longer matches" % file_id)
            self._removeCachedIds(cachehits, cache)
            return self._getFile(client, path, storageid, cache)

        return file

    def GetFileContents(self, svcRec, client, path, storageid, cache):
        """ Return a tuple of (contents, version_number) for a given path. """
        import hashlib

        file = self._getFile(client, path, storageid, cache)
        if file is None or file.get("downloadUrl") is None:
            # File not found or has no contents
            return None, 0

        resp, content = client._http.request(file.get("downloadUrl"))
        if resp.status != 200:
            raise APIException("Google drive download error - status %d" % resp.status)

        md5sum = file.get("md5Checksum")
        if md5sum:
            csp = hashlib.new("md5")
            csp.update(content)
            contentmd5 = csp.hexdigest()
            if contentmd5.lower() != md5sum.lower():
                raise APIException("Google drive download error - md5 mismatch %s vs %s" % (md5sum, contentmd5))
        return content, file["version"]

    def PutFileContents(self, svcRec, client, path, contents, cache):
        """ Write the contents to the file and return a version number for the newly written file. """
        fname = _basename(path)
        parent = path[:-(len(fname)+1)]
        logger.debug("Google Drive putting file contents for %s %s" % (parent, fname))
        parent_id, cachehits = self._getFileId(client, parent, cache)

        if parent_id is None:
            # First make a directory.  Only make one level up.
            dirname = _basename(parent)
            top_parent = parent[:-(len(dirname)+1)]
            logger.debug("Google Drive creating parent - '%s' '%s'" % (top_parent, dirname))
            top_parent_id, topcachehits = self._getFileId(client, top_parent, cache)
            if top_parent_id is None:
                raise APIException("Parent of directory for %s does not exist, giving up" % (path,))

            body = {"title": dirname, "mimeType": _FOLDER_MIMETYPE, "parents": [{"id": top_parent_id}]}

            try:
                parent_obj = client.files().insert(body=body).execute()
            except errors.HttpError as error:
                if error.resp.status == 404 and topcachehits:
                    logger.debug("Google drive cache %s invalid - 404" % top_parent_id)
                    self._removeCachedIds(topcachehits.union(cachehits), cache)  # remove cache entries and try again
                    return self.PutFileContents(svcRec, client, path, contents, cache)
                raise APIException("Google drive error creating folder - %s" % error)

            parent_id = parent_obj["id"]

        extn = fname.split(".")[-1].lower()
        if extn not in _MIMETYPES:
            # Shouldn't happen?
            raise APIException("Google drive upload only supports file types %s" % (_MIMETYPES.keys(),))

        media_body = MediaInMemoryUpload(contents, mimetype=_MIMETYPES[extn], resumable=True)
        # TODO: Maybe description should ideally be Activity.Notes?
        body = {"title": fname, "description": "Uploaded by Tapiriik", "mimeType": _MIMETYPES[extn], "parents": [{"id": parent_id}]}

        try:
            file = client.files().insert(body=body, media_body=media_body).execute()
            return file["version"]
        except errors.HttpError as error:
            if error.resp.status == 404 and cachehits:
                logger.debug("Google drive cache %s invalid - 404" % parent_id)
                self._removeCachedIds(cachehits, cache)  # remove cache entries and try again
                return self.PutFileContents(svcRec, client, path, contents, cache)
            raise APIException("Google drive upload error - %s" % error)

    def MoveFile(self, svcRec, client, path, destPath, cache):
        """ Move/rename the file "path" to "destPath". """
        fname1 = _basename(path)
        fname2 = _basename(destPath)
        if path[:-len(fname1)] != destPath[:-len(fname2)]:
            # Currently only support renaming files in the same dir, otherwise
            # we have to twiddle parents which is hard..
            raise NotImplementedError()

        try:
            file = self._getFile(client, path, cache)
            if file is None:
                raise APIException("Error renaming file: %s not found" % path)
            file["title"] = fname1
            client.files().update(fileId=file["id"], body=file, newRevision=False).execute()
        except errors.HttpError as error:
            raise APIException("Error renaming file: %s" % error)

    def ServiceCacheDB(self):
        return cachedb.googledrive_cache

    def SyncRoot(self, svcRec):
        # TODO: Make this configurable
        return "/tapiriik"

    def EnumerateFiles(self, svcRec, client, root, cache):
        root_id, cachehits = self._getFileId(client, root, cache)
        if root_id is None:
            # Root does not exist.. that's ok, just no files to list.
            return

        idcache = self._idCache(cache)
        yield from self._folderRecurse(svcRec, client, root_id, root, idcache)

    def _folderRecurse(self, svcRec, client, parent_id, parent_path, id_cache):
        assert(not parent_path.endswith("/"))
        page_token = None
        while True:
            try:
                param = {"maxResults": 1000, "q": "trashed = false and '%s' in parents" % parent_id, "fields": "items(id,version,parents(id,isRoot,kind),title,md5Checksum,mimeType),kind,nextLink,nextPageToken"}
                if page_token:
                    param["pageToken"] = page_token
                children = client.files().list(**param).execute()

                for child in children.get("items", []):
                    ctitle = child["title"]
                    cid = child["id"]
                    cpath = parent_path + "/" + ctitle
                    is_folder = child.get("mimeType") == _FOLDER_MIMETYPE
                    is_supported_file = any([ctitle.lower().endswith("."+x) for x in _MIMETYPES.keys()])

                    if not is_folder and not is_supported_file:
                        continue

                    cache_entry = {"ID": cid, "Parent": parent_id, "Name": ctitle}
                    if cache_entry not in id_cache:
                        if any([x["ID"] == cid for x in id_cache]):
                            # Cached different name or parent info for this ID, maybe moved
                            logger.debug("ID %s seems to have changed name, updating cache" % cid)
                            id_cache[:] = (x for x in id_cache if x["ID"] != cid)
                        if any([x["Parent"] == parent_id and x["Name"] == ctitle for x in id_cache]):
                            logger.debug("%s/%s seems to have changed id, updating cache" % (parent_id, ctitle))
                            # Cached different info for this parent/name
                            id_cache[:] = (x for x in id_cache if not (x["Parent"] == parent_id and x["Name"] != ctitle))
                        id_cache.append(cache_entry)

                    if is_folder:
                        yield from self._folderRecurse(svcRec, client, cid, cpath, id_cache)
                    elif is_supported_file:
                        yield (cpath, cpath.replace(parent_path, "", 1), cid, child["version"])

                page_token = children.get("nextPageToken")
                if not page_token:
                    break
            except errors.HttpError as error:
                raise APIException("Error listing files in Google Drive - %s" % error)
