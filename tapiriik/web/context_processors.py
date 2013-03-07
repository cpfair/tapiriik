from tapiriik.services import Service
from tapiriik.sync import Sync
from tapiriik.settings import SITE_VER, PP_WEBSCR
from tapiriik.database import db


def providers(req):
    return {"service_providers": Service.List()}


def config(req):
    return {"config": {"minimumSyncInterval": Sync.MinimumSyncInterval.seconds, "siteVer": SITE_VER, "pp_url": PP_WEBSCR}}


def stats(req):
    return {"stats": db.stats.find_one()}
