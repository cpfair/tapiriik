from .service_authentication import *
from .api import *
from tapiriik.services.RunKeeper import RunKeeperService
RunKeeper = RunKeeperService()
from tapiriik.services.Strava import StravaService
Strava = StravaService()
from tapiriik.services.Endomondo import EndomondoService
Endomondo = EndomondoService()
from tapiriik.services.Dropbox import DropboxService
Dropbox = DropboxService()
from .service import *
