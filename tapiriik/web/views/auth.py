from django.http import HttpResponse
from django.shortcuts import render, redirect
from tapiriik.services import Service
from tapiriik.auth import User


def auth_login(req, service):
    if "password" in req.POST:
        res = auth_do(req, service)
        if res:
            return redirect("dashboard")

    return render(req,"auth/login.html",{"serviceid":service,"service":Service.FromID(service)});

def auth_do(req, service):
    svc = Service.FromID(service)
    uid, authData = svc.Authorize(req.POST["username"], req.POST["password"])
    if authData is not None:
        serviceRecord = Service.EnsureServiceRecordWithAuthDetails(svc, uid, authData)
        # auth by this service connection
        existingUser = User.AuthByService(serviceRecord)
        if existingUser is not None:
            User.Login(existingUser, req)
        else:
            User.Ensure(req)
        # link service to user account, possible merge happens behind the scenes (but doesn't effect active user)
        User.ConnectService(req.user, serviceRecord)
        return True
    return False