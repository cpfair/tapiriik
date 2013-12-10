class ServiceExceptionScope:
    Account = "account"
    Service = "service"

class ServiceException(Exception):
    def __init__(self, message, scope=ServiceExceptionScope.Service, block=False, user_exception=None):
        Exception.__init__(self, message)
        self.Message = message
        self.UserException = user_exception
        self.Block = block
        self.Scope = scope

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
    def __init__(self, message, activity=None, activityId=None, permanent=True):
        Exception.__init__(self, message)
        self.Message = message
        self.Activity = activity
        self.ExternalActivityID = activityId
        self.Permanent = permanent

    def __str__(self):
        return self.Message + " (activity " + str(self.ExternalActivityID) + ")"

class UserExceptionType:
    Authorization = "auth"
    AccountFull = "full"
    AccountExpired = "expired"
    AccountUnpaid = "unpaid" # vs. expired, which implies it was at some point function, via payment or trial or otherwise.

class UserException:
    def __init__(self, type, extra=None, intervention_required=False, clear_group=None):
        self.Type = type
        self.Extra = extra # Unimplemented - displayed as part of the error message.
        self.InterventionRequired = intervention_required # Does the user need to dismiss this error?
        self.ClearGroup = clear_group if clear_group else type # Used to group error messages displayed to the user, and let them clear a group that share a common cause.
