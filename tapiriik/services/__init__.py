from .service_base import *
from .api import *
from tapiriik.services.RunKeeper import RunKeeperService
RunKeeper = RunKeeperService()
from tapiriik.services.Strava import StravaService
Strava = StravaService()
from tapiriik.services.Endomondo import EndomondoService
Endomondo = EndomondoService()
from tapiriik.services.Dropbox import DropboxService
Dropbox = DropboxService()
from tapiriik.services.GarminConnect import GarminConnectService
GarminConnect = GarminConnectService()
from tapiriik.services.SportTracks import SportTracksService
SportTracks = SportTracksService()
from .service import *
from .service_record import *
