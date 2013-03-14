import json
from django.http import HttpResponse
from tapiriik.auth import User
from tapiriik.services import Service


def config_save(req, service):
    if not req.user:
        return HttpResponse(status=403)

    conn = User.GetConnectionRecord(req.user, service)
    if not conn:
        return HttpResponse(status=404)
    print(json.loads(req.POST["config"]))
    Service.SetConfiguration(json.loads(req.POST["config"]), conn)
    return HttpResponse()
