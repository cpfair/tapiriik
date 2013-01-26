#reinventing the interface here
from tapiriik.services import *

class Service:
    def FromID(id):
        if id=="runkeeper":
            return RunKeeper
        elif id=="strava":
            return Strava
        raise ValueError
    def List():
        return [RunKeeper, Strava]
    def WebInit():
        global UserAuthorizationURL
        for itm in Service.List():
            itm.WebInit()
            print("post" + itm.UserAuthorizationURL)
