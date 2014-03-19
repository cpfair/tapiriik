from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.shortcuts import redirect
from tapiriik.auth import User


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
	User.SetConfiguration(req.user, req.POST)
	return HttpResponse()