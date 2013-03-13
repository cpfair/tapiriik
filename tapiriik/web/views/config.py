import json
from django.http import HttpResponse
from tapiriik.auth import User


def config_save(req, svc):
	if not req.user:
        return HttpResponse(status=403)

    conn = User.GetConnectionRecord(req.user, svc)
    if not conn:
    	return HttpResponse(status=404)

	Service.SetConfiguration(json.loads(req.POST["config"]), conn)
	return HttpResponse()
