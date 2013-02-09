class APIException(Exception):
    def __init__(self, message, connectionRecord):
        Exception.__init__(self, message)
        self.Connection = connectionRecord
        self.Message = message

    def __str__(self):
        return self.Message + " (connection " + str(self.Connection) + " )"


class APIAuthorizationException(APIException):
    pass
