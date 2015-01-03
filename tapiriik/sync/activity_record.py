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
        self.FailureCounts = {}

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

    def MarkAsPresentOn(self, serviceRecord):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(listTimestamp=datetime.utcnow())
        else:
            self.PresentOnServices[serviceRecord.Service.ID].ProcessedTimestamp = datetime.utcnow()
        if serviceRecord.Service.ID in self.NotPresentOnServices:
            del self.NotPresentOnServices[serviceRecord.Service.ID]

    def MarkAsSynchronizedTo(self, serviceRecord):
        if serviceRecord.Service.ID not in self.PresentOnServices:
            self.PresentOnServices[serviceRecord.Service.ID] = ActivityServicePrescence(syncTimestamp=datetime.utcnow())
        else:
            self.PresentOnServices[serviceRecord.Service.ID].SynchronizedTimestamp = datetime.utcnow()
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

    # Only unexpected, "active" failures increment this count
    #
    # Exceptions in the sync core (activity type unsupported, etc) cost nothing => don't count towards this, to keep things simple
    # Trapped errors inside Services use...
    #  - Activity exclusions, which either...
    #    a) permanently blacklist that activity => don't count towards this, to avoid duplication
    #    b) temporarily exclude it, where there's a finite horizon for success (activiteies being live-tracked, generally) => don't count toward this
    #  - APIExceptions, Exceptions => DO increment this
    #
    # These are stored seperately from the prescences, since the efficiency gained with how MarkAsNotPresentOtherwise works
    # would prevent keeping track of activity download failures (couldn't determine which service to blame after the fact)
    #
    # We don't store the error that originated the failure - that's already being stored in the service record
    # When it comes time to reset these counts once a widespread issue is solved, one can determine the appropriate users from that
    #
    # Also, we don't track the Step here - I can probably reverse-engineer it from the prescence (famous last words) if reqd

    def GetFailureCount(self, serviceRecord):
        return self.FailureCounts[serviceRecord.Service.ID] if serviceRecord.Service.ID in self.FailureCounts else 0

    def IncrementFailureCount(self, serviceRecord):
        self.FailureCounts[serviceRecord.Service.ID] = self.GetFailureCount(serviceRecord) + 1

    def ResetFailureCount(self, serviceRecord):
        if serviceRecord.Service.ID in self.FailureCounts:
            del self.FailureCounts[serviceRecord.Service.ID]



class ActivityServicePrescence:
    def __init__(self, listTimestamp=None, syncTimestamp=None, userException=None):
        self.ProcessedTimestamp = listTimestamp
        self.SynchronizedTimestamp = syncTimestamp
        # If these is a UserException then this object is actually indicating the abscence of an activity from a service.
        if userException is not None and not isinstance(userException, UserException):
            raise ValueError("Provided UserException %s is not a UserException" % userException)
        self.UserException = userException

