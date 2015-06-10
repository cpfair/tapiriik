from django.shortcuts import render
from tapiriik.services import Service
from tapiriik.settings import WITHDRAWN_SERVICES, SOFT_LAUNCH_SERVICES
from tapiriik.auth import User
def privacy(request):

    OPTIN = "<span class=\"optin policy\">Opt-in</span>"
    NO = "<span class=\"no policy\">No</span>"
    YES = "<span class=\"yes policy\">Yes</span>"
    CACHED = "<span class=\"cached policy\">Cached</span>"
    SEEBELOW = "See below"

    services = dict([[x.ID, {"DisplayName": x.DisplayName, "ID": x.ID}] for x in Service.List() if x.ID not in WITHDRAWN_SERVICES])

    services["garminconnect"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["strava"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":NO})
    services["sporttracks"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":NO})
    services["dropbox"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":CACHED})
    services["runkeeper"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":NO})
    services["rwgps"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["trainingpeaks"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["endomondo"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":NO})
    services["motivato"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["nikeplus"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["velohero"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["runsense"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["trainerroad"].update({"email": OPTIN, "password": OPTIN, "tokens": NO, "metadata": YES, "data":NO})
    services["smashrun"].update({"email": NO, "password": NO, "tokens": YES, "metadata": YES, "data":NO})

    for svc_id in SOFT_LAUNCH_SERVICES:
        if svc_id in services:
            del services[svc_id]

    def user_services_sort(service):
        if not request.user:
            return 0
        if User.IsServiceConnected(request.user, service["ID"]):
            return 0
        else:
            return 1

    services_list = sorted(services.values(), key=user_services_sort)
    return render(request, "privacy.html", {"services": services_list})
