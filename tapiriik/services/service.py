#reinventing the interface here
from tapiriik.services import *

class Service:
    def FromID(id):
        if id=="runkeeper":
            return RunKeeper
        raise ValueError
    def List():
        return [RunKeeper]
    def WebInit():
        global UserAuthorizationURL
        for itm in Service.List():
            itm.WebInit()
            print("post" + itm.UserAuthorizationURL)


class ServiceAuthenticationType:
    OAuth = 1
    UsernamePassword = 666  # it is, believe me


class OAuthService:
    AuthenticationType = ServiceAuthenticationType.OAuth
