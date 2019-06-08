class ServiceExceptionScope:
    Account = "account"
    Service = "service"
    # Unlike Account and Service-level blocking exceptions, these are implemented via ActivityRecord.FailureCounts
    # Eventually, all errors might be stored in ActivityRecords
    Activity = "activity"

class ServiceException(Exception):
    def __init__(self, message, scope=ServiceExceptionScope.Service, block=False, user_exception=None, trigger_exhaustive=True):
        Exception.__init__(self, message)
        self.Message = message
        self.UserException = user_exception
        self.Block = block
        self.Scope = scope
        self.TriggerExhaustive = trigger_exhaustive

    def __str__(self):
        return self.Message + " (user " + str(self.UserException) + " )"

class ServiceWarning(ServiceException):
    pass

class APIException(ServiceException):
    pass

class APIWarning(ServiceWarning):
    pass

# Theoretically, APIExcludeActivity should actually be a ServiceException with block=True, scope=Activity
# It's on the to-do list.

class APIExcludeActivity(Exception):
    def __init__(self, message, activity=None, activity_id=None, permanent=True, user_exception=None):
        Exception.__init__(self, message)
        self.Message = message
        self.Activity = activity
        self.ExternalActivityID = activity_id
        self.Permanent = permanent
        self.UserException = user_exception

    def __str__(self):
        return self.Message + " (activity " + str(self.ExternalActivityID) + ")"

class UserExceptionType:
    # Account-level exceptions (not a hardcoded thing, just to keep these seperate)
    Authorization = "auth"
    RenewPassword = "renew_password"
    Locked = "locked"
    AccountFull = "full"
    AccountExpired = "expired"
    AccountUnpaid = "unpaid" # vs. expired, which implies it was at some point function, via payment or trial or otherwise.
    NonAthleteAccount = "non_athlete_account" # trainingpeaks
    GCUploadConsent = "gc_upload_consent" # EU User must grant upload consent on GC

    # Activity-level exceptions
    FlowException = "flow"
    Private = "private"
    NoSupplier = "nosupplier"
    NotTriggered = "notrigger"
    Deferred = "deferred" # They've instructed us not to synchronize activities for some time after they complete
    PredatesWindow = "predates_window" # They've instructed us not to synchronize activities before some date
    RateLimited = "ratelimited"
    MissingCredentials = "credentials_missing" # They forgot to check the "Remember these details" box
    NotConfigured = "config_missing" # Don't think this error is even possible any more.
    StationaryUnsupported = "stationary"
    NonGPSUnsupported = "nongps"
    TypeUnsupported = "type_unsupported"
    InsufficientData = "data_insufficient" # Some services demand more data than others provide (ahem, N+)
    DownloadError = "download"
    ListingError = "list" # Cases when a service fails listing, so nothing can be uploaded to it.
    UploadError = "upload"
    SanityError = "sanity"
    Corrupt = "corrupt" # Kind of a scary term for what's generally "some data is missing"
    Untagged = "untagged"
    LiveTracking = "live"
    UnknownTZ = "tz_unknown"
    System = "system"
    Other = "other"

class UserException:
    def __init__(self, type, extra=None, intervention_required=False, clear_group=None):
        self.Type = type
        self.Extra = extra # Unimplemented - displayed as part of the error message.
        self.InterventionRequired = intervention_required # Does the user need to dismiss this error?
        self.ClearGroup = clear_group if clear_group else type # Used to group error messages displayed to the user, and let them clear a group that share a common cause.
