from tapiriik.database import cachedb

class ServiceRecord:
    def __new__(cls, dbRec):
        if not dbRec:
            return None
        return super(ServiceRecord, cls).__new__(cls)
    def __init__(self, dbRec):
        self.__dict__.update(dbRec)
    def __repr__(self):
        return "<ServiceRecord> " + str(self.__dict__)

    def __eq__(self, other):
        return self._id == other._id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __deepcopy__(self, x):
        return ServiceRecord(self.__dict__)

    @property
    def Service(self):
        from tapiriik.services import Service
        return Service.FromID(self.__dict__["Service"])

    def HasExtendedAuthorizationDetails(self):
        if not self.Service.RequiresExtendedAuthorizationDetails:
            return False
        if "ExtendedAuthorization" in self.__dict__ and self.ExtendedAuthorization:
            return True
        return cachedb.extendedAuthDetails.find({"ID": self._id}).limit(1).count()
