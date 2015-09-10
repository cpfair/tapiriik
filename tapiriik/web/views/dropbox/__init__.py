from django.shortcuts import redirect, render
from django.http import HttpResponse
from tapiriik.services import Service
from tapiriik.auth import User
import json

def browse(req, path="/"):

    if req.user is None:
        return HttpResponse(status=403)
    svcRec = User.GetConnectionRecord(req.user, "dropbox")
    dbSvc = Service.FromID("dropbox")
    dbCl = dbSvc._getClient(svcRec)
    metadata = dbCl.metadata(path)
    folders = []
    for item in metadata["contents"]:
        if item["is_dir"] is False:
            continue
        folders.append(item["path"])
    return HttpResponse(json.dumps(folders), content_type='application/json')
