from django.shortcuts import redirect
from tapiriik.services import Service
from tapiriik.auth import User
def authreturn(req, service):
    svc = Service.FromID(service)
    authRecord = svc.RetrieveAuthenticationToken(req)
    serviceRecord = Service.GetServiceRecord(svc, authRecord)
    # auth by this service connection
    existingUser = User.AuthByService(serviceRecord)
    if existingUser is not None:
        User.Login(existingUser, req)
    else:
        User.Ensure(req)

    # link service to user account, possible merge happens behind the scenes (but doesn't effect active user)
    User.ConnectService(req.user, serviceRecord)
    
    return redirect("dashboard")
