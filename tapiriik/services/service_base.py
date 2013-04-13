class ServiceAuthenticationType:
    OAuth = "oauth"
    UsernamePassword = "direct"


class ServiceBase:
    ID = AuthenticationType = DisplayName = SupportedActivities = None
    Configurable = RequiresConfiguration = False  # requiresConfiguration means no sync until user configures
    SupportsHR = SupportsCalories = SupportsCadence = SupportsTemp = SupportsPower = False
    UserAuthorizationURL = None
    UserProfileURL = None
    AuthenticationNoFrame = False
    ConfigurationDefaults = {}

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

    def ConfigurationUpdating(self, newConfig, oldConfig):
        raise NotImplementedError
