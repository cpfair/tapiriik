from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from tapiriik.settings import INVITE_KEYS


@ensure_csrf_cookie
def dashboard(req):

    if len(INVITE_KEYS) > 0:
        if "invite" in req.GET:
                req.session["invite"] = req.GET["invite"]

        inviteKey = req.session.get("invite")
        if inviteKey is None or inviteKey not in INVITE_KEYS:
            return render(req, "site-splash.html")

    user = req.user
    user["ID"] = user["_id"]

    return render(req, "dashboard.html", {"user": user})
