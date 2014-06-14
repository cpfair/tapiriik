from datetime import datetime
from tapiriik.services.interchange import ActivityStatisticUnit
from tapiriik.services.api import UserException

class ActivityRecord:
    def __init__(self, dbRec=None):
        self.StartTime = None
        self.EndTime = None
        self.Name = None
        self.Notes = None
        self.Type = None
        self.Distance = None
        self.Stationary = None
        self.Private = None
        self.UIDs = []
        self.PresentOnServices = {}
        self.NotPresentOnServices = {}

        # It's practically an ORM!
        if dbRec:
            self.__dict__.update(dbRec)

    def __repr__(self):
        return "<ActivityRecord> " + str(self.__dict__)

    def __deepcopy__(self, x):
        return ActivityRecord(self.__dict__)

    def FromActivity(activity):
        record = ActivityRecord()
        record.SetActivity(activity)
        return record

    def SetActivity(self, activity):
        self.StartTime = activity.StartTime
        self.EndTime = activity.EndTime
        self.Name = activity.Name
        self.Notes = activity.Notes
        self.Type = activity.Type
        self.Distance = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value
        self.Stationary = activity.Stationary
        self.Private = activity.Private
        self.UIDs = activity.UIDs

    def MarkAsPresentOn(self, serviceRecord, serviceKeys=set()):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(listTimestamp=datetime.utcnow(), serviceKeys=serviceKeys)
        else:
            self.PresentOnServices[serviceRecord.Service.ID].ProcessedTimestamp = datetime.utcnow()
            self.PresentOnServices[serviceRecord.Service.ID].ServiceKeys.update(serviceKeys)
        if serviceRecord.Service.ID in self.NotPresentOnServices:
            del self.NotPresentOnServices[serviceRecord.Service.ID]

    def MarkAsSynchronizedTo(self, serviceRecord, serviceKeys=set()):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(syncTimestamp=datetime.utcnow(), serviceKeys=serviceKeys)
        else:
            self.PresentOnServices[serviceRecord.Service.ID].SynchronizedTimestamp = datetime.utcnow()
            self.PresentOnServices[serviceRecord.Service.ID].ServiceKeys.update(serviceKeys)
        if serviceRecord.Service.ID in self.NotPresentOnServices:
            del self.NotPresentOnServices[serviceRecord.Service.ID]

    def MarkAsNotPresentOtherwise(self, userException):
        self.MarkAsNotPresentOn(None, userException)

    def MarkAsNotPresentOn(self, serviceRecord, userException):
        rec_id = serviceRecord.Service.ID if serviceRecord else None
        if rec_id not in self.NotPresentOnServices:
            self.NotPresentOnServices[rec_id] = ActivityServicePrescence(listTimestamp=datetime.utcnow(), userException=userException)
        else:
            record = self.NotPresentOnServices[rec_id]
            record.ProcessedTimestamp = datetime.utcnow()
            record.UserException = userException


class ActivityServicePrescence:
    def __init__(self, listTimestamp=None, syncTimestamp=None, userException=None, serviceKeys=set()):
        self.ProcessedTimestamp = listTimestamp
        self.SynchronizedTimestamp = syncTimestamp
        self.ServiceKeys = set(serviceKeys)
        # If these is a UserException then this object is actually indicating the abscence of an activity from a service.
        if userException is not None and not isinstance(userException, UserException):
            raise ValueError("Provided UserException %s is not a UserException" % userException)
        self.UserException = userException

