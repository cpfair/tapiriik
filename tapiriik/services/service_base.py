class ServiceAuthenticationType:
    OAuth = "oauth"
    UsernamePassword = "direct"

class InvalidServiceOperationException(Exception):
    pass

class ServiceBase:
    # Short ID used everywhere in logging and DB storage
    ID = None
    # Full display name given to users
    DisplayName = None
    # 2-3 letter abbreviated name
    DisplayAbbreviation = None

    # One of ServiceAuthenticationType
    AuthenticationType = None

    # Enables extended auth ("Save these details") functionality
    RequiresExtendedAuthorizationDetails = False

    # URL to direct user to when starting authentication
    UserAuthorizationURL = None

    # Don't attempt to IFrame the OAuth login
    AuthenticationNoFrame = False

    # List of ActivityTypes
    SupportedActivities = None

    # Used only in tests
    SupportsHR = SupportsCalories = SupportsCadence = SupportsTemp = SupportsPower = False

    # Does it?
    ReceivesStationaryActivities = True
    ReceivesNonGPSActivitiesWithOtherSensorData = True

    # Causes synchronizations to be skipped until...
    #  - One is triggered (via IDs returned by ServiceRecordIDsForPartialSyncTrigger or PollPartialSyncTrigger)
    #  - One is necessitated (non-partial sync, possibility of uploading new activities, etc)
    PartialSyncRequiresTrigger = False
    # Timedelta for polling to happen at (or None for no polling)
    PartialSyncTriggerPollInterval = None
    # How many times to call the polling method per interval (this is for the multiple_index kwarg)
    PartialSyncTriggerPollMultiple = 1

    @property
    def PartialSyncTriggerRequiresPolling(self):
        return self.PartialSyncRequiresTrigger and self.PartialSyncTriggerPollInterval

    # Adds the Setup button to the service configuration pane, and not much else
    Configurable = False
    # Defaults for per-service configuration
    ConfigurationDefaults = {}

    # For the diagnostics dashboard
    UserProfileURL = UserActivityURL = None

    def RequiresConfiguration(self, serviceRecord):  # Should convert this into a real property
        return False  # True means no sync until user configures

    def WebInit(self):
        pass

    def GenerateUserAuthorizationURL(self, level=None):
        raise NotImplementedError

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

    def SubscribeToPartialSyncTrigger(self, serviceRecord):
        if self.PartialSyncRequiresTrigger:
            raise NotImplementedError
        else:
            raise InvalidServiceOperationException

    def UnsubscribeFromPartialSyncTrigger(self, serviceRecord):
        if self.PartialSyncRequiresTrigger:
            raise NotImplementedError
        else:
            raise InvalidServiceOperationException

    def ShouldForcePartialSyncTrigger(self, serviceRecord):
        if self.PartialSyncRequiresTrigger:
            return False
        else:
            raise InvalidServiceOperationException

    def PollPartialSyncTrigger(self, multiple_index):
        if self.PartialSyncRequiresTrigger and self.PartialSyncTriggerPollInterval:
            raise NotImplementedError
        else:
            raise InvalidServiceOperationException

    def ServiceRecordIDsForPartialSyncTrigger(self, req):
        raise NotImplementedError

    def ConfigurationUpdating(self, serviceRecord, newConfig, oldConfig):
        pass
