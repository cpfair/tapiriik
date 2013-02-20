from django.shortcuts import render, redirect
from django.http import HttpResponse
from tapiriik.settings import DIAG_AUTH_TOTP_SECRET, DIAG_AUTH_PASSWORD
import hashlib
import onetimepass

def diag_requireAuth(view):
    def authWrapper(req, *args, **kwargs):
        if "diag_auth" not in req.session or req.session["diag_auth"] != True:
            return redirect("diagnostics_login")
        return view(req, *args, **kwargs)
    return authWrapper


@diag_requireAuth
def diag_dashboard(req, user=None):
    pass


def diag_login(req):
    if "password" in req.POST:
        if hashlib.sha512(req.POST["password"].encode("utf-8")).hexdigest().upper() == DIAG_AUTH_PASSWORD and onetimepass.valid_totp(req.POST["totp"], DIAG_AUTH_TOTP_SECRET):
            req.session["diag_auth"] = True
            return redirect("diagnostics_dashboard")
    return render(req, "diag/login.html")