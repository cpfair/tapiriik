from tapiriik.services.ratelimiting import RateLimit, RateLimitExceededException
from tapiriik.services.api import ServiceException, UserExceptionType, UserException

class ServiceAuthenticationType:
    OAuth = "oauth"
    UsernamePassword = "direct"

class InvalidServiceOperationException(Exception):
    pass

class ServiceBase:
    # Short ID used everywhere in logging and DB storage
    ID = None
    # Alias ID in case somebody (not naming names) typoed a service name and needs to keep the old ID functional
    IDAliases = None
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
    ReceivesActivities = True # Any at all?
    ReceivesStationaryActivities = True # Manually-entered?
    ReceivesNonGPSActivitiesWithOtherSensorData = True # Trainer-ish?
    SuppliesActivities = True
    # Services with this flag unset will receive an explicit date range for activity listing,
    # rather than the exhaustive flag alone. They are also processed after all other services.
    # An account must have at least one service that supports exhaustive listing.
    SupportsExhaustiveListing = True


    SupportsActivityDeletion = False


    # Causes synchronizations to be skipped until...
    #  - One is triggered (via IDs returned by ExternalIDsForPartialSyncTrigger or PollPartialSyncTrigger)
    #  - One is necessitated (non-partial sync, possibility of uploading new activities, etc)
    PartialSyncRequiresTrigger = False
    PartialSyncTriggerRequiresSubscription = False
    PartialSyncTriggerStatusCode = 204
    # Timedelta for polling to happen at (or None for no polling)
    PartialSyncTriggerPollInterval = None
    # How many times to call the polling method per interval (this is for the multiple_index kwarg)
    PartialSyncTriggerPollMultiple = 1

    # How many times should we try each operation on an activity before giving up?
    # (only ever tries once per sync run - so ~1 hour interval on average)
    UploadRetryCount = 5
    DownloadRetryCount = 5

    # Global rate limiting options
    # For when there's a limit on the API key itself
    GlobalRateLimits = []
    # Preemptively sleep to avoid hitting the limits
    GlobalRateLimitsPreemptiveSleep = False

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

    # Return an URL pointing directly to the specified activity on the remote site
    def UserUploadedActivityURL(self, uploadId):
        raise NotImplementedError

    def GenerateUserAuthorizationURL(self, session, level=None):
        raise NotImplementedError

    def Authorize(self, email, password, store=False):
        raise NotImplementedError

    def RevokeAuthorization(self, serviceRecord):
        raise NotImplementedError

    def DownloadActivityList(self, serviceRecord, exhaustive_start_date=None):
        raise NotImplementedError

    def DownloadActivity(self, serviceRecord, activity):
        raise NotImplementedError

    # Should return an uploadId for storage and potential use in DeleteActivity
    def UploadActivity(self, serviceRecord, activity):
        raise NotImplementedError

    def DeleteActivity(self, serviceRecord, uploadId):
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

    def ExternalIDsForPartialSyncTrigger(self, req):
        raise NotImplementedError

    def PartialSyncTriggerGET(self, req):
        from django.http import HttpResponse
        return HttpResponse(status=204)

    def ConfigurationUpdating(self, serviceRecord, newConfig, oldConfig):
        pass

    def _globalRateLimit(self):
        try:
            RateLimit.Limit(self.ID, self.GlobalRateLimits if self.GlobalRateLimitsPreemptiveSleep else ())
        except RateLimitExceededException:
            raise ServiceException("Global rate limit reached", user_exception=UserException(UserExceptionType.RateLimited))

