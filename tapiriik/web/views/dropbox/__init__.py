from django.shortcuts import redirect, render
from django.http import HttpResponse
from tapiriik.services import Service
from tapiriik.auth import User
import json

def browse(req):
    if req.user is None:
        return HttpResponse(status=403)
    path = req.GET.get("path", "")
    if path == "/":
        path = ""
    svcRec = User.GetConnectionRecord(req.user, "dropbox")
    dbSvc = Service.FromID("dropbox")
    dbCl = dbSvc._getClient(svcRec)

    folders = []
    result = dbCl.files_list_folder(path)
    while True:
        # There's no actual way to filter for folders only :|
        folders += [x.path_lower for x in result.entries if not hasattr(x, "rev")]
        if result.has_more:
            result = dbCl.files_list_folder_continue(result.cursor)
        else:
            break

    return HttpResponse(json.dumps(sorted(folders)), content_type='application/json')
