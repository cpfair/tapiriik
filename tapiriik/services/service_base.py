class ServiceAuthenticationType:
    OAuth = "oauth"
    UsernamePassword = "direct"


class ServiceBase:
    ID = AuthenticationType = DisplayName = SupportedActivities = None
    Configurable = RequiresConfiguration = False
    SupportsHR = SupportsCalories = SupportsCadence = SupportsTemp = SupportsPower = False
    UserAuthorizationURL = None

    def WebInit(self):
        pass

    def Authorize(self, email, password):
        raise NotImplementedError

    def RevokeAuthorization(self, serviceRecord):
        raise NotImplementedError

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        raise NotImplementedError

    def DownloadActivity(self, serviceRecord, activity):
        raise NotImplementedError

    def UploadActivity(self, serviceRecord, activity):
        raise NotImplementedError

    def DeleteCachedData(self, serviceRecord):
        raise NotImplementedError
