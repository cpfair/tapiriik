class ServiceAuthenticationType:
    OAuth = "oauth"
    UsernamePassword = "direct"


class ServiceBase:
    ID = AuthenticationType = DisplayName = SupportedActivities = None
    RequiresExtendedAuthorizationDetails = False
    Configurable = False

    def RequiresConfiguration(self, serviceRecord):  # this needs to be a pseudo-property since Dropbox needs to decide at runtime
        return False  # true means no sync until user configures

    SupportsHR = SupportsCalories = SupportsCadence = SupportsTemp = SupportsPower = False
    UserAuthorizationURL = None
    UserProfileURL = None
    AuthenticationNoFrame = False
    ConfigurationDefaults = {}

    def WebInit(self):
        pass

    def Authorize(self, email, password, store=False):
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
