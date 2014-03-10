from tapiriik.database import cachedb, db
import copy

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

    ExcludedActivities = {}
    Config = {}
    PartialSyncTriggerSubscribed = False

    @property
    def Service(self):
        from tapiriik.services import Service
        return Service.FromID(self.__dict__["Service"])

    def HasExtendedAuthorizationDetails(self, persisted_only=False):
        if not self.Service.RequiresExtendedAuthorizationDetails:
            return False
        if "ExtendedAuthorization" in self.__dict__ and self.ExtendedAuthorization:
            return True
        if persisted_only:
            return False
        return cachedb.extendedAuthDetails.find({"ID": self._id}).limit(1).count()

    def SetPartialSyncTriggerSubscriptionState(self, subscribed):
        db.connections.update({"_id": self._id}, {"$set": {"PartialSyncTriggerSubscribed": subscribed}})

    def GetConfiguration(self):
        from tapiriik.services import Service
        svc = self.Service
        config = copy.deepcopy(Service._globalConfigurationDefaults)
        config.update(svc.ConfigurationDefaults)
        config.update(self.Config)
        return config

    def SetConfiguration(self, config, no_save=False, drop_existing=False):
        from tapiriik.services import Service
        sparseConfig = {}
        if not drop_existing:
            sparseConfig = copy.deepcopy(self.GetConfiguration())
        sparseConfig.update(config)

        svc = self.Service
        svc.ConfigurationUpdating(self, config, self.GetConfiguration())
        for k, v in config.items():
            if (k in svc.ConfigurationDefaults and svc.ConfigurationDefaults[k] == v) or (k in Service._globalConfigurationDefaults and Service._globalConfigurationDefaults[k] == v):
                del sparseConfig[k]  # it's the default, we can not store it
        self.Config = sparseConfig
        if not no_save:
            db.connections.update({"_id": self._id}, {"$set": {"Config": sparseConfig}})
