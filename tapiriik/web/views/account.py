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
