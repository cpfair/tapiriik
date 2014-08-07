from tapiriik.sync import Sync
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.shortcuts import redirect
from tapiriik.auth import User
import json
import dateutil.parser


@require_POST
def account_setemail(req):
    if not req.user:
        return HttpResponse(status=403)
    User.SetEmail(req.user, req.POST["email"])
    return redirect("dashboard")

@require_POST
def account_settimezone(req):
    if not req.user:
        return HttpResponse(status=403)
    User.SetTimezone(req.user, req.POST["timezone"])
    return HttpResponse()

@require_POST
def account_setconfig(req):
    if not req.user:
        return HttpResponse(status=403)
    data = json.loads(req.body.decode("utf-8"))
    if data["sync_skip_before"] and len(data["sync_skip_before"]):
        data["sync_skip_before"] = dateutil.parser.parse(data["sync_skip_before"])
    User.SetConfiguration(req.user, data)
    Sync.SetNextSyncIsExhaustive(req.user, True)
    return HttpResponse()