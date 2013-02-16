from tapiriik.services import Service
from tapiriik.sync import Sync


def providers(req):
    return {"service_providers": Service.List()}


def config(req):
    return {"config": {"minimumSyncInterval": Sync.MinimumSyncInterval.seconds}}
