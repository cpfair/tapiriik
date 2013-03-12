from tapiriik.services import Service
from tapiriik.auth import User
from tapiriik.sync import Sync
from tapiriik.settings import SITE_VER
from tapiriik.database import db
import json


def providers(req):
    return {"service_providers": Service.List()}


def config(req):
    return {"config": {"minimumSyncInterval": Sync.MinimumSyncInterval.seconds, "siteVer": SITE_VER}}


def js_bridge(req):
    serviceInfo = {}
    for svc in Service.List():
        svcRec = User.GetConnectionRecord(req.user, svc.ID)  # maybe make the auth handler do this only once?
        info = {
            "AuthenticationType": svc.AuthenticationType,
            "AuthorizationURL": svc.UserAuthorizationURL,
            "NoFrame": svc.AuthenticationNoFrame,
            "Configurable": svc.Configurable,
            "RequiresConfiguration": svc.RequiresConfiguration
        }
        if svc.Configurable:
            info["Config"] = {"Configured": Service.HasConfiguration(svcRec)}
            if info["Config"]["Configured"]:
                info["Config"]["Configuration"] = Service.GetConfiguration(svcRec)
        serviceInfo[svc.ID] = info
    return {"js_bridge_serviceinfo": json.dumps(serviceInfo)}


def stats(req):
    return {"stats": db.stats.find_one()}
