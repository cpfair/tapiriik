from .service_authentication import *
from .api import *
from tapiriik.services.RunKeeper import RunKeeperService
RunKeeper = RunKeeperService()
from tapiriik.services.Strava import StravaService
Strava = StravaService()
from tapiriik.services.MapMyFitness import MapMyFitnessService
MapMyFitness = MapMyFitnessService()
from .service import *
