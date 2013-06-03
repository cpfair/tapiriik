class ServiceException(Exception):
    def __init__(self, message, code=None):
        Exception.__init__(self, message)
        self.Message = message
        self.Code = code

    def __str__(self):
        return self.Message + " (code " + str(self.Code) + " )"

class ServiceWarning(ServiceException):
    pass

class APIException(ServiceException):
    pass

class APIWarning(ServiceWarning):
    pass

class APIExcludeActivity(Exception):
    def __init__(self, message, activity=None, activityId=None, permanent=True):
        Exception.__init__(self, message)
        self.Message = message
        self.Activity = activity
        self.ExternalActivityID = activityId
        self.Permanent = permanent

    def __str__(self):
        return self.Message + " (activity " + str(self.ExternalActivityID) + ")"

class APIAuthorizationException(APIException):
    pass
