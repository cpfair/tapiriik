from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django.shortcuts import render, redirect
from tapiriik.services import Service
from tapiriik.auth import User
import json


def auth_login(req, service):
    if "password" in req.POST:
        res = auth_do(req, service)
        if res:
            return redirect("dashboard")
    return render(req, "auth/login.html", {"serviceid": service, "service": Service.FromID(service)})


def auth_login_ajax(req, service):
    res = auth_do(req, service)
    return HttpResponse(json.dumps({"success": res}), mimetype='application/json')


def auth_do(req, service):
    svc = Service.FromID(service)
    from tapiriik.services.api import APIException
    try:
        if svc.RequiresExtendedAuthorizationDetails:
            uid, authData, extendedAuthData = svc.Authorize(req.POST["username"], req.POST["password"])
        else:
            uid, authData = svc.Authorize(req.POST["username"], req.POST["password"])
    except APIException:
        return False
    if authData is not None:
        serviceRecord = Service.EnsureServiceRecordWithAuth(svc, uid, authData, extendedAuthDetails=extendedAuthData if svc.RequiresExtendedAuthorizationDetails else None, persistExtendedAuthDetails=bool(req.POST.get("persist", None)))
        # auth by this service connection
        existingUser = User.AuthByService(serviceRecord)
        # only log us in as this different user in the case that we don't already have an account
        if existingUser is not None and req.user is None:
            User.Login(existingUser, req)
        else:
            User.Ensure(req)
        # link service to user account, possible merge happens behind the scenes (but doesn't effect active user)
        User.ConnectService(req.user, serviceRecord)
        return True
    return False


def auth_disconnect(req, service):
    if not req.user:
        return redirect("dashboard")
    if "action" in req.POST:
        if req.POST["action"] == "disconnect":
            auth_disconnect_do(req, service)
        return redirect("dashboard")
    return render(req, "auth/disconnect.html", {"serviceid": service, "service": Service.FromID(service)})


@csrf_protect
@require_POST  # don't want this getting called by just anything
def auth_disconnect_ajax(req, service):
    try:
        status = auth_disconnect_do(req, service)
    except Exception as e:
        return HttpResponse(json.dumps({"success": False, "error": str(e)}), mimetype='application/json', status=500)
    return HttpResponse(json.dumps({"success": status}), mimetype='application/json')


def auth_disconnect_do(req, service):
    svc = Service.FromID(service)
    svcId = [x["ID"] for x in req.user["ConnectedServices"] if x["Service"] == svc.ID]
    if len(svcId) == 0:
        return
    else:
        svcId = svcId[0]
    svcRec = Service.GetServiceRecordByID(svcId)
    Service.DeleteServiceRecord(svcRec)
    User.DisconnectService(svcRec)
    return True
