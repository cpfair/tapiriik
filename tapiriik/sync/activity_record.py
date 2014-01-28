import datetime
class ActivityRecord:
    def __init__(self, dbRec=None, activity=None):
        # It's practically an ORM!
        if dbRec:
            self.__dict__.update(dbRec)
        if activity:
            self.FromActivity(activity)

    def __repr__(self):
        return "<ActivityRecord> " + str(self.__dict__)

    def __deepcopy__(self, x):
        return ActivityRecord(self.__dict__)

    StartTime = None
    Name = None
    Notes = None
    Type = None
    PresentOnServices = {}
    NotPresentOnServices = {}

    def FromActivity(self, activity):
        self.StartTime = activity.StartTime
        self.Name = activity.Name
        self.Notes = activity.Notes
        self.Type = activity.Type
        # We miiiight be able to populate PresentOnServices here, but at the price of a lot of coupling.

    def MarkAsPresentOn(self, serviceRecord):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(listTimestamp=datetime.utcnow())
        else:
            self.PresentOnServices[serviceRecord.Service.ID].ProcessedTimestamp = datetime.utcnow()

    def MarkAsSynchronizedTo(self, serviceRecord):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(syncTimestamp=datetime.utcnow()s)
        else:
            self.PresentOnServices[serviceRecord.Service.ID].SynchonizedTimestamp = datetime.utcnow()

    def MarkAsNotPresentOtherwise(self, userException):
        self.MarkAsPresentOn(None, type, extra, attemptedSync)

    def MarkAsNotPresentOn(self, serviceRecord, userException):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(listTimestamp=datetime.utcnow(), userException=userException)
        else:
            record = self.PresentOnServices[serviceRecord.Service.ID]
            record.ProcessedTimestamp = datetime.utcnow()
            record.UserException = userException


class ActivityServicePrescence:
    def __init__(self, listTimestamp=None, syncTimestamp=None, userException=None):
        self.ProcessedTimestamp = listTimestamp
        self.SynchonizedTimestamp = syncTimestamp
        # If these is a UserException then this object is actually indicating the abscence of an activity from a service.
        self.UserException = userException
