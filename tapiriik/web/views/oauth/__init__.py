from django.http import HttpResponse
from tapiriik.services import Service
from tapiriik.database import db
def authreturn(req, service):
    svc = Service.FromID(service)
    token = svc.RetrieveAuthenticationToken(req)
    serviceRecord = db.connections.find_one({"AuthorizationToken":token, "Service":service})
    if serviceRecord is None:
        db.connections.insert({"AuthorizationToken":token, "Service":service})
    return HttpResponse(token)
