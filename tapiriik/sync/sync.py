from tapiriik.database import db, cachedb
from tapiriik.services import Service, ServiceRecord, APIExcludeActivity, ServiceException, ServiceExceptionScope, ServiceWarning, UserException, UserExceptionType
from tapiriik.settings import USER_SYNC_LOGS, DISABLED_SERVICES, WITHDRAWN_SERVICES
from .activity_record import ActivityRecord, ActivityServicePrescence
from datetime import datetime, timedelta
import sys
import os
import socket
import traceback
import pprint
import copy
import random
import logging
import logging.handlers
import pytz

# Set this up seperate from the logger used in this scope, so services logging messages are caught and logged into user's files.
_global_logger = logging.getLogger("tapiriik")

_global_logger.setLevel(logging.DEBUG)
logging_console_handler = logging.StreamHandler(sys.stdout)
logging_console_handler.setLevel(logging.DEBUG)
logging_console_handler.setFormatter(logging.Formatter('%(message)s'))
_global_logger.addHandler(logging_console_handler)

logger = logging.getLogger("tapiriik.sync.worker")

def _formatExc():
    try:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = exc_traceback
        while tb.tb_next:
            tb = tb.tb_next
        frame = tb.tb_frame
        locals_trimmed = []
        for local_name, local_val in frame.f_locals.items():
            value_full = pprint.pformat(local_val)
            if len(value_full) > 1000:
                value_full = value_full[:500] + "..." + value_full[-500:]
            locals_trimmed.append(str(local_name) + "=" + value_full)
        exc = '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + "\nLOCALS:\n" + '\n'.join(locals_trimmed)
        logger.exception("Service exception")
        return exc
    finally:
        del exc_traceback, exc_value, exc_type

# It's practically an ORM!

def _packServiceException(step, e):
    res = {"Step": step, "Message": e.Message + "\n" + _formatExc(), "Block": e.Block, "Scope": e.Scope}
    if e.UserException:
        res["UserException"] = _packUserException(e.UserException)
    return res

def _packUserException(userException):
    if userException:
        return {"Type": userException.Type, "Extra": userException.Extra, "InterventionRequired": userException.InterventionRequired, "ClearGroup": userException.ClearGroup}

def _unpackUserException(raw):
    if not raw:
        return None
    if "UserException" in raw:
        raw = raw["UserException"]
    if not raw:
        return None
    if "Type" not in raw:
        return None
    return UserException(raw["Type"], extra=raw["Extra"], intervention_required=raw["InterventionRequired"], clear_group=raw["ClearGroup"])

class Sync:

    SyncInterval = timedelta(hours=1)
    SyncIntervalJitter = timedelta(minutes=5)
    MinimumSyncInterval = timedelta(seconds=30)
    MaximumIntervalBeforeExhaustiveSync = timedelta(days=14)  # Based on the general page size of 50 activites, this would be >3/day...

    def ScheduleImmediateSync(user, exhaustive=None):
        if exhaustive is None:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})
        else:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow(), "NextSyncIsExhaustive": exhaustive}})

    def SetNextSyncIsExhaustive(user, exhaustive=False):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSyncIsExhaustive": exhaustive}})

    def PerformGlobalSync(heartbeat_callback=None, version=None):
        from tapiriik.auth import User
        users = db.users.find({
                "NextSynchronization": {"$lte": datetime.utcnow()},
                "SynchronizationWorker": None,
                "$or": [
                    {"SynchronizationHostRestriction": {"$exists": False}},
                    {"SynchronizationHostRestriction": socket.gethostname()}
                    ]
            }).sort("NextSynchronization").limit(1)
        userCt = 0
        for user in users:
            userCt += 1
            syncStart = datetime.utcnow()

            # Always to an exhaustive sync if there were errors
            #   Sometimes services report that uploads failed even when they succeeded.
            #   If a partial sync was done, we'd be assuming that the accounts were consistent past the first page
            #       e.g. If an activity failed to upload far in the past, it would never be attempted again.
            #   So we need to verify the full state of the accounts.
            # But, we can still do a partial sync if there are *only* blocking errors
            #   In these cases, the block will protect that service from being improperly manipulated (though tbqh I can't come up with a situation where this would happen, it's more of a performance thing).
            #   And, when the block is cleared, NextSyncIsExhaustive is set.

            exhaustive = "NextSyncIsExhaustive" in user and user["NextSyncIsExhaustive"] is True
            if "NonblockingSyncErrorCount" in user and user["NonblockingSyncErrorCount"] > 0:
                exhaustive = True

            try:
                Sync.PerformUserSync(user, exhaustive, null_next_sync_on_unlock=True, heartbeat_callback=heartbeat_callback)
            except SynchronizationConcurrencyException:
                pass  # another worker picked them
            else:
                nextSync = None
                if User.HasActivePayment(user):
                    nextSync = datetime.utcnow() + Sync.SyncInterval + timedelta(seconds=random.randint(-Sync.SyncIntervalJitter.total_seconds(), Sync.SyncIntervalJitter.total_seconds()))
                db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": nextSync, "LastSynchronization": datetime.utcnow(), "LastSynchronizationVersion": version}, "$unset": {"NextSyncIsExhaustive": None}})
                syncTime = (datetime.utcnow() - syncStart).total_seconds()
                db.sync_worker_stats.insert({"Timestamp": datetime.utcnow(), "Worker": os.getpid(), "Host": socket.gethostname(), "TimeTaken": syncTime})
        return userCt

    def PerformUserSync(user, exhaustive=False, null_next_sync_on_unlock=False, heartbeat_callback=None):
        SynchronizationTask(user).Run(exhaustive=exhaustive, null_next_sync_on_unlock=null_next_sync_on_unlock, heartbeat_callback=heartbeat_callback)


class SynchronizationTask:
    _logFormat = '[%(levelname)-8s] %(asctime)s (%(name)s:%(lineno)d) %(message)s'
    _logDateFormat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, user):
        self.user = user

    def _lockUser(self):
        db.users.update({"_id": self.user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname(), "SynchronizationStartTime": datetime.utcnow()}})
        lockCheck = db.users.find_one({"_id": self.user["_id"], "SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

    def _unlockUser(self, null_next_sync_on_unlock):
        update_values = {"$unset": {"SynchronizationWorker": None}}
        if null_next_sync_on_unlock:
            # Sometimes another worker would pick this record in the timespan between this update and the one in PerformGlobalSync that sets the true next sync time.
            # Hence, an option to unset the NextSynchronization in the same operation that releases the lock on the row.
            update_values["$unset"]["NextSynchronization"] = None
        db.users.update({"_id": self.user["_id"], "SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname()}, update_values)

    def _loadServiceData(self):
        self._connectedServiceIds = [x["ID"] for x in self.user["ConnectedServices"]]
        self._serviceConnections = [ServiceRecord(x) for x in db.connections.find({"_id": {"$in": self._connectedServiceIds}})]

    def _updateSyncProgress(self, step, progress):
        db.users.update({"_id": self.user["_id"]}, {"$set": {"SynchronizationProgress": progress, "SynchronizationStep": step}})

    def _initializeUserLogging(self):
        self._logging_file_handler = logging.handlers.RotatingFileHandler(USER_SYNC_LOGS + str(self.user["_id"]) + ".log", maxBytes=0, backupCount=10)
        self._logging_file_handler.setFormatter(logging.Formatter(self._logFormat, self._logDateFormat))
        self._logging_file_handler.doRollover()
        _global_logger.addHandler(self._logging_file_handler)

    def _closeUserLogging(self):
        _global_logger.removeHandler(self._logging_file_handler)
        self._logging_file_handler.flush()
        self._logging_file_handler.close()

    def _loadExtendedAuthData(self):
        self._extendedAuthDetails = list(cachedb.extendedAuthDetails.find({"ID": {"$in": self._connectedServiceIds}}))

    def _destroyExtendedAuthData(self):
        cachedb.extendedAuthDetails.remove({"ID": {"$in": self._connectedServiceIds}})

    def _initializePersistedSyncErrorsAndExclusions(self):
        self._syncErrors = {}
        self._syncExclusions = {}

        for conn in self._serviceConnections:
            if hasattr(conn, "SyncErrors"):
                # Remove non-blocking errors
                self._syncErrors[conn._id] = [x for x in conn.SyncErrors if "Block" in x and x["Block"]]
                del conn.SyncErrors
            else:
                self._syncErrors[conn._id] = []

            # Remove temporary exclusions (live tracking etc).
            self._syncExclusions[conn._id] = dict((k, v) for k, v in (conn.ExcludedActivities if conn.ExcludedActivities else {}).items() if v["Permanent"])

            if conn.ExcludedActivities:
                del conn.ExcludedActivities  # Otherwise the exception messages get really, really, really huge and break mongodb.

    def _writeBackSyncErrorsAndExclusions(self):
        nonblockingSyncErrorsCount = 0
        blockingSyncErrorsCount = 0
        syncExclusionCount = 0
        for conn in self._serviceConnections:
            update_values = {
                "$set": {
                    "SyncErrors": self._syncErrors[conn._id],
                    "ExcludedActivities": self._syncExclusions[conn._id]
                }
            }

            if not self._isServiceExcluded(conn):
                # Only reset the trigger if we succesfully got through the entire sync without bailing on this particular connection
                update_values["$unset"] = {"TriggerPartialSync": None}

            db.connections.update({"_id": conn._id}, update_values)
            nonblockingSyncErrorsCount += len([x for x in self._syncErrors[conn._id] if "Block" not in x or not x["Block"]])
            blockingSyncErrorsCount += len([x for x in self._syncErrors[conn._id] if "Block" in x and x["Block"]])
            syncExclusionCount += len(self._syncExclusions[conn._id].items())

        db.users.update({"_id": self.user["_id"]}, {"$set": {"NonblockingSyncErrorCount": nonblockingSyncErrorsCount, "BlockingSyncErrorCount": blockingSyncErrorsCount, "SyncExclusionCount": syncExclusionCount}})

    def _writeBackActivityRecords(self):
        def _activityPrescences(prescences):
            return dict([(svcId if svcId else "",
                {
                    "Processed": presc.ProcessedTimestamp,
                    "Synchronized": presc.SynchronizedTimestamp,
                    "Exception": _packUserException(presc.UserException)
                }) for svcId, presc in prescences.items()])

        self._activityRecords.sort(key=lambda x: x.StartTime.replace(tzinfo=None), reverse=True)
        composed_records = [
            {
                "StartTime": x.StartTime,
                "EndTime": x.EndTime,
                "Type": x.Type,
                "Name": x.Name,
                "Notes": x.Notes,
                "Private": x.Private,
                "Stationary": x.Stationary,
                "Distance": x.Distance,
                "UIDs": list(x.UIDs),
                "Prescence": _activityPrescences(x.PresentOnServices),
                "Abscence": _activityPrescences(x.NotPresentOnServices)
            }
            for x in self._activityRecords
        ]

        db.activity_records.update(
            {"UserID": self.user["_id"]},
            {
                "$set": {
                    "UserID": self.user["_id"],
                    "Activities": composed_records
                }
            },
            upsert=True
        )

    def _initializeActivityRecords(self):
        raw_records = db.activity_records.find_one({"UserID": self.user["_id"]})
        self._activityRecords = []
        if not raw_records:
            return
        else:
            raw_records = raw_records["Activities"]
            for raw_record in raw_records:
                if "UIDs" not in raw_record:
                    continue # From the few days where this was rolled out without this key...
                rec = ActivityRecord(raw_record)
                rec.UIDs = set(rec.UIDs)
                # Did I mention I should really start using an ORM-type deal any day now?
                for svc, absent in rec.Abscence.items():
                    rec.NotPresentOnServices[svc] = ActivityServicePrescence(absent["Processed"], absent["Synchronized"], _unpackUserException(absent["Exception"]))
                for svc, present in rec.Prescence.items():
                    rec.PresentOnServices[svc] = ActivityServicePrescence(present["Processed"], present["Synchronized"], _unpackUserException(present["Exception"]))
                del rec.Prescence
                del rec.Abscence
                rec.Touched = False
                self._activityRecords.append(rec)

    def _findOrCreateActivityRecord(self, activity):
        for record in self._activityRecords:
            if record.UIDs & activity.UIDs:
                record.Touched = True
                return record
        record = ActivityRecord.FromActivity(activity)
        record.Touched = True
        self._activityRecords.append(record)
        return record

    def _dropUntouchedActivityRecords(self):
        self._activityRecords[:] = [x for x in self._activityRecords if x.Touched]

    def _excludeService(self, serviceRecord, userException):
        self._excludedServices[serviceRecord._id] = userException if userException else None

    def _isServiceExcluded(self, serviceRecord):
        return serviceRecord._id in self._excludedServices

    def _getServiceExclusionUserException(self, serviceRecord):
        return self._excludedServices[serviceRecord._id]

    def _determineRecipientServices(self, activity):
        recipientServices = []
        for conn in self._serviceConnections:
            if conn._id in activity.ServiceDataCollection:
                # The activity record is updated earlier for these, blegh.
                pass
            elif hasattr(conn, "SynchronizedActivities") and len([x for x in activity.UIDs if x in conn.SynchronizedActivities]):
                pass
            elif activity.Type not in conn.Service.SupportedActivities:
                logger.debug("\t...%s doesn't support type %s" % (conn.Service.ID, activity.Type))
                activity.Record.MarkAsNotPresentOn(conn, UserException(UserExceptionType.TypeUnsupported))
            else:
                recipientServices.append(conn)
        return recipientServices

    def _coalesceDatetime(self, a, b, knownTz=None):
        """ Returns the most informative (TZ-wise) datetime of those provided - defaulting to the first if they are equivalently descriptive """
        if not b:
            if knownTz and a and not a.tzinfo:
                return a.replace(tzinfo=knownTz)
            return a
        if not a:
            if knownTz and b and not b.tzinfo:
                return b.replace(tzinfo=knownTz)
            return b
        if a.tzinfo and not b.tzinfo:
            return a
        elif b.tzinfo and not a.tzinfo:
            return b
        else:
            if knownTz and not a.tzinfo:
                return a.replace(tzinfo=knownTz)
            return a

    def _accumulateActivities(self, conn, svcActivities, no_add=False):
        # Yep, abs() works on timedeltas
        activityStartLeeway = timedelta(minutes=3)
        activityStartTZOffsetLeeway = timedelta(seconds=10)
        timezoneErrorPeriod = timedelta(hours=38)
        from tapiriik.services.interchange import ActivityType
        for act in svcActivities:
            act.UIDs = set([act.UID])
            if not hasattr(act, "ServiceDataCollection"):
                act.ServiceDataCollection = {}
            if hasattr(act, "ServiceData") and act.ServiceData is not None:
                act.ServiceDataCollection[conn._id] = act.ServiceData
                del act.ServiceData
            if act.TZ and not hasattr(act.TZ, "localize"):
                raise ValueError("Got activity with TZ type " + str(type(act.TZ)) + " instead of a pytz timezone")
            # Used to ensureTZ() right here - doubt it's needed any more?
            existElsewhere = [
                              x for x in self._activities if
                              (
                                  # Identical
                                  x.UID == act.UID
                                  or
                                  # Check to see if the self._activities are reasonably close together to be considered duplicate
                                  (x.StartTime is not None and
                                   act.StartTime is not None and
                                   (act.StartTime.tzinfo is not None) == (x.StartTime.tzinfo is not None) and
                                   abs(act.StartTime-x.StartTime) < activityStartLeeway
                                  )
                                  or
                                  # Try comparing the time as if it were TZ-aware and in the expected TZ (this won't actually change the value of the times being compared)
                                  (x.StartTime is not None and
                                   act.StartTime is not None and
                                   (act.StartTime.tzinfo is not None) != (x.StartTime.tzinfo is not None) and
                                   abs(act.StartTime.replace(tzinfo=None)-x.StartTime.replace(tzinfo=None)) < activityStartLeeway
                                  )
                                  or
                                  # Sometimes wacky stuff happens and we get two activities with the same mm:ss but different hh, because of a TZ issue somewhere along the line.
                                  # So, we check for any activities +/- 14, wait, 38 hours that have the same minutes and seconds values.
                                  #  (14 hours because Kiribati, and later, 38 hours because of some really terrible import code that existed on a service that shall not be named).
                                  # There's a very low chance that two activities in this period would intersect and be merged together.
                                  # But, given the fact that most users have maybe 0.05 activities per this period, it's an acceptable tradeoff.
                                  (x.StartTime is not None and
                                   act.StartTime is not None and
                                   abs(act.StartTime.replace(tzinfo=None)-x.StartTime.replace(tzinfo=None)) < timezoneErrorPeriod and
                                   abs(act.StartTime.replace(tzinfo=None).replace(hour=0) - x.StartTime.replace(tzinfo=None).replace(hour=0)) < activityStartTZOffsetLeeway
                                   )
                                  or
                                  # Similarly, for half-hour time zones (there are a handful of quarter-hour ones, but I've got to draw a line somewhere, even if I revise it several times)
                                  (x.StartTime is not None and
                                   act.StartTime is not None and
                                   abs(act.StartTime.replace(tzinfo=None)-x.StartTime.replace(tzinfo=None)) < timezoneErrorPeriod and
                                   abs(act.StartTime.replace(tzinfo=None).replace(hour=0) - x.StartTime.replace(tzinfo=None).replace(hour=0)) > timedelta(minutes=30) - (activityStartTZOffsetLeeway / 2) and
                                   abs(act.StartTime.replace(tzinfo=None).replace(hour=0) - x.StartTime.replace(tzinfo=None).replace(hour=0)) < timedelta(minutes=30) + (activityStartTZOffsetLeeway / 2)
                                   )
                               )
                                and
                                # Prevents closely-spaced activities of known different type from being lumped together - esp. important for manually-enetered ones
                                (x.Type == ActivityType.Other or act.Type == ActivityType.Other or x.Type == act.Type or ActivityType.AreVariants([act.Type, x.Type]))
                              ]
            if len(existElsewhere) > 0:
                existingActivity = existElsewhere[0]
                # we don't merge the exclude values here, since at this stage the services have the option of just not returning those activities
                if act.TZ is not None and existingActivity.TZ is None:
                    existingActivity.TZ = act.TZ
                    existingActivity.DefineTZ()
                existingActivity.FallbackTZ = existingActivity.FallbackTZ if existingActivity.FallbackTZ else act.FallbackTZ
                # tortuous merging logic is tortuous
                existingActivity.StartTime = self._coalesceDatetime(existingActivity.StartTime, act.StartTime)
                existingActivity.EndTime = self._coalesceDatetime(existingActivity.EndTime, act.EndTime, knownTz=existingActivity.StartTime.tzinfo)
                existingActivity.Name = existingActivity.Name if existingActivity.Name else act.Name
                existingActivity.Notes = existingActivity.Notes if existingActivity.Notes else act.Notes
                existingActivity.Laps = existingActivity.Laps if len(existingActivity.Laps) > len(act.Laps) else act.Laps
                existingActivity.Type = ActivityType.PickMostSpecific([existingActivity.Type, act.Type])
                existingActivity.Private = existingActivity.Private or act.Private
                if act.Stationary is not None:
                    if existingActivity.Stationary is None:
                        existingActivity.Stationary = act.Stationary
                    else:
                        existingActivity.Stationary = existingActivity.Stationary and act.Stationary # Let's be optimistic here
                else:
                    pass # Nothing to do - existElsewhere is either more speicifc or equivalently indeterminate
                existingActivity.Stats.coalesceWith(act.Stats)

                serviceDataCollection = dict(act.ServiceDataCollection)
                serviceDataCollection.update(existingActivity.ServiceDataCollection)
                existingActivity.ServiceDataCollection = serviceDataCollection

                existingActivity.UIDs |= act.UIDs  # I think this is merited
                act.UIDs = existingActivity.UIDs  # stop the circular inclusion, not that it matters
                continue
            if not no_add:
                self._activities.append(act)

    def _determineEligibleRecipientServices(self, activity, recipientServices):
        from tapiriik.auth import User
        eligibleServices = []
        for destinationSvcRecord in recipientServices:
            if self._isServiceExcluded(destinationSvcRecord):
                logger.info("\t\tExcluded " + destinationSvcRecord.Service.ID)
                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, self._getServiceExclusionUserException(destinationSvcRecord))
                continue  # we don't know for sure if it needs to be uploaded, hold off for now
            flowException = True

            sources = [[y for y in self._serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()]
            for src in sources:
                if src.Service.ID in WITHDRAWN_SERVICES:
                    continue # They can't see this service to change the configuration.
                if not User.CheckFlowException(self.user, src, destinationSvcRecord):
                    flowException = False
                    break

            if flowException:
                logger.info("\t\tFlow exception for " + destinationSvcRecord.Service.ID)
                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.FlowException))
                continue

            destSvc = destinationSvcRecord.Service
            if destSvc.RequiresConfiguration(destinationSvcRecord):
                logger.info("\t\t" + destSvc.ID + " not configured")
                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.NotConfigured))
                continue  # not configured, so we won't even try
            if not destSvc.ReceivesStationaryActivities and activity.Stationary:
                logger.info("\t\t" + destSvc.ID + " doesn't receive stationary activities")
                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.StationaryUnsupported))
                continue # Missing this originally, no wonder...
            eligibleServices.append(destinationSvcRecord)
        return eligibleServices

    def _accumulateExclusions(self, serviceRecord, exclusions):
        if type(exclusions) is not list:
            exclusions = [exclusions]
        for exclusion in exclusions:
            identifier = exclusion.Activity.UID if exclusion.Activity else exclusion.ExternalActivityID
            if not identifier:
                raise ValueError("Activity excluded with no identifying information")
            identifier = str(identifier).replace(".", "_")
            self._syncExclusions[serviceRecord._id][identifier] = {"Message": exclusion.Message, "Activity": str(exclusion.Activity) if exclusion.Activity else None, "ExternalActivityID": exclusion.ExternalActivityID, "Permanent": exclusion.Permanent, "Effective": datetime.utcnow(), "UserException": _packUserException(exclusion.UserException)}

    def _ensurePartialSyncPollingSubscription(self, conn):
        if conn.Service.PartialSyncTriggerRequiresPolling and not conn.PartialSyncTriggerSubscribed:
            if conn.Service.RequiresExtendedAuthorizationDetails and not conn.ExtendedAuthorization:
                return # We (probably) can't subscribe unless we have their credentials. May need to change this down the road.
            conn.Service.SubscribeToPartialSyncTrigger(conn)

    def _primeExtendedAuthDetails(self, conn):
        if conn.Service.RequiresExtendedAuthorizationDetails:
            if not hasattr(conn, "ExtendedAuthorization") or not conn.ExtendedAuthorization:
                extAuthDetails = [x["ExtendedAuthorization"] for x in self._extendedAuthDetails if x["ID"] == conn._id]
                if not len(extAuthDetails):
                    conn.ExtendedAuthorization = None
                    return
                # The connection never gets saved in full again, so we can sub these in here at no risk.
                conn.ExtendedAuthorization = extAuthDetails[0]

    def _downloadActivityList(self, conn, exhaustive, no_add=False):
        svc = conn.Service
        # Bail out as appropriate for the entire account (_syncErrors contains only blocking errors at this point)
        if [x for x in self._syncErrors[conn._id] if x["Scope"] == ServiceExceptionScope.Account]:
            raise SynchronizationCompleteException()

        # ...and for this specific service
        if [x for x in self._syncErrors[conn._id] if x["Scope"] == ServiceExceptionScope.Service]:
            logger.info("Service %s is blocked:" % conn.Service.ID)
            self._excludeService(conn, _unpackUserException([x for x in self._syncErrors[conn._id] if x["Scope"] == ServiceExceptionScope.Service][0]))
            return

        if svc.ID in DISABLED_SERVICES or svc.ID in WITHDRAWN_SERVICES:
            logger.info("Service %s is widthdrawn" % conn.Service.ID)
            self._excludeService(conn, UserException(UserExceptionType.Other))
            return

        if svc.RequiresExtendedAuthorizationDetails:
            if not conn.ExtendedAuthorization:
                logger.info("No extended auth details for " + svc.ID)
                self._excludeService(conn, UserException(UserExceptionType.MissingCredentials))
                return

        try:
            logger.info("\tRetrieving list from " + svc.ID)
            svcActivities, svcExclusions = svc.DownloadActivityList(conn, exhaustive)
        except (ServiceException, ServiceWarning) as e:
            self._syncErrors[conn._id].append(_packServiceException(SyncStep.List, e))
            self._excludeService(conn, e.UserException)
            if not issubclass(e.__class__, ServiceWarning):
                return
        except Exception as e:
            self._syncErrors[conn._id].append({"Step": SyncStep.List, "Message": _formatExc()})
            self._excludeService(conn, UserException(UserExceptionType.ListingError))
            return
        self._accumulateExclusions(conn, svcExclusions)
        self._accumulateActivities(conn, svcActivities, no_add=no_add)

    def _estimateFallbackTZ(self, activities):
        from collections import Counter
        # With the hope that the majority of the activity records returned will have TZs, and the user's current TZ will constitute the majority.
        TZOffsets = [x.StartTime.utcoffset().total_seconds() / 60 for x in activities if x.TZ is not None]
        mode = Counter(TZOffsets).most_common(1)
        if not len(mode):
            if "Timezone" in self.user:
                return pytz.timezone(self.user["Timezone"])
            return None
        return pytz.FixedOffset(mode[0][0])

    def _applyFallbackTZ(self):
        # Attempt to assign fallback TZs to all stationary/potentially-stationary activities, since we may not be able to determine TZ any other way.
        fallbackTZ = self._estimateFallbackTZ(self._activities)
        if fallbackTZ:
            logger.info("Setting fallback TZs to %s" % fallbackTZ )
            for act in self._activities:
                act.FallbackTZ = fallbackTZ

    def _processActivityOrigins(self):
        logger.info("Reading activity origins")
        origins = list(db.activity_origins.find({"ActivityUID": {"$in": [x.UID for x in self._activities]}}))
        activitiesWithOrigins = [x["ActivityUID"] for x in origins]

        logger.info("Populating origins")
        # Populate origins
        for activity in self._activities:
            if len(activity.ServiceDataCollection.keys()) == 1:
                if not len(self._excludedServices):  # otherwise it could be incorrectly recorded
                    # we can log the origin of this activity
                    if activity.UID not in activitiesWithOrigins:  # No need to hammer the database updating these when they haven't changed
                        logger.info("\t\t Updating db with origin for proceeding activity")
                        db.activity_origins.insert({"ActivityUID": activity.UID, "Origin": {"Service": [[y for y in self._serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0].Service.ID, "ExternalID": [[y.ExternalID for y in self._serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0]}})
                    activity.Origin = [[y for y in self._serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0]
            else:
                if activity.UID in activitiesWithOrigins:
                    knownOrigin = [x for x in origins if x["ActivityUID"] == activity.UID]
                    connectedOrigins = [x for x in self._serviceConnections if knownOrigin[0]["Origin"]["Service"] == x.Service.ID and knownOrigin[0]["Origin"]["ExternalID"] == x.ExternalID]
                    if len(connectedOrigins) > 0:  # they might have disconnected it
                        activity.Origin = connectedOrigins[0]
                    else:
                        activity.Origin = ServiceRecord(knownOrigin[0]["Origin"])  # I have it on good authority that this will work

    def _updateSynchronizedActivities(self, activity):
        # Locally mark this activity as present on the appropriate services.
        # These needs to happen regardless of whether the activity is going to be synchronized.
        #   Before, I had moved this under all the eligibility/recipient checks, but that could cause persistent duplicate self._activities when the user had already manually uploaded the same activity to multiple sites.
        updateServicesWithExistingActivity = False
        for serviceWithExistingActivityId in activity.ServiceDataCollection.keys():
            serviceWithExistingActivity = [x for x in self._serviceConnections if x._id == serviceWithExistingActivityId][0]
            if not hasattr(serviceWithExistingActivity, "SynchronizedActivities") or not (activity.UIDs <= set(serviceWithExistingActivity.SynchronizedActivities)):
                updateServicesWithExistingActivity = True
                break

        if updateServicesWithExistingActivity:
            logger.debug("\t\tUpdating SynchronizedActivities")
            db.connections.update({"_id": {"$in": list(activity.ServiceDataCollection.keys())}},
                                  {"$addToSet": {"SynchronizedActivities": {"$each": list(activity.UIDs)}}},
                                  multi=True)

    def _updateActivityRecordInitialPrescence(self, activity):
        for connWithExistingActivityId in activity.ServiceDataCollection.keys():
            connWithExistingActivity = [x for x in self._serviceConnections if x._id == connWithExistingActivityId][0]
            activity.Record.MarkAsPresentOn(connWithExistingActivity)
        for conn in self._serviceConnections:
            if hasattr(conn, "SynchronizedActivities") and len([x for x in activity.UIDs if x in conn.SynchronizedActivities]):
                activity.Record.MarkAsPresentOn(conn)

    def _downloadActivity(self, activity):
        act = None
        actAvailableFromSvcIds = activity.ServiceDataCollection.keys()
        actAvailableFromSvcs = [[x for x in self._serviceConnections if x._id == dlSvcRecId][0] for dlSvcRecId in actAvailableFromSvcIds]

        servicePriorityList = Service.PreferredDownloadPriorityList()
        actAvailableFromSvcs.sort(key=lambda x: servicePriorityList.index(x.Service))

        # TODO: redo this, it was completely broken:
        # Prefer retrieving the activity from its original source.

        for dlSvcRecord in actAvailableFromSvcs:
            dlSvc = dlSvcRecord.Service
            logger.info("\tfrom " + dlSvc.ID)
            if activity.UID in self._syncExclusions[dlSvcRecord._id]:
                activity.Record.MarkAsNotPresentOtherwise(_unpackUserException(self._syncExclusions[dlSvcRecord._id][activity.UID]))
                logger.info("\t\t...has activity exclusion logged")
                continue
            if self._isServiceExcluded(dlSvcRecord):
                activity.Record.MarkAsNotPresentOtherwise(self._getServiceExclusionUserException(dlSvcRecord))
                logger.info("\t\t...service became excluded after listing") # Because otherwise we'd never have been trying to download from it in the first place.
                continue

            workingCopy = copy.copy(activity)  # we can hope
            # Load in the service data in the same place they left it.
            workingCopy.ServiceData = workingCopy.ServiceDataCollection[dlSvcRecord._id] if dlSvcRecord._id in workingCopy.ServiceDataCollection else None
            try:
                workingCopy = dlSvc.DownloadActivity(dlSvcRecord, workingCopy)
            except (ServiceException, ServiceWarning) as e:
                self._syncErrors[dlSvcRecord._id].append(_packServiceException(SyncStep.Download, e))
                if e.Block and e.Scope == ServiceExceptionScope.Service: # I can't imagine why the same would happen at the account level, so there's no behaviour to immediately abort the sync in that case.
                    self._excludeService(dlSvcRecord, e.UserException)
                if not issubclass(e.__class__, ServiceWarning):
                    activity.Record.MarkAsNotPresentOtherwise(e.UserException)
                    continue
            except APIExcludeActivity as e:
                logger.info("\t\texcluded by service: %s" % e.Message)
                e.Activity = workingCopy
                self._accumulateExclusions(dlSvcRecord, e)
                activity.Record.MarkAsNotPresentOtherwise(e.UserException)
                continue
            except Exception as e:
                self._syncErrors[dlSvcRecord._id].append({"Step": SyncStep.Download, "Message": _formatExc()})
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.DownloadError))
                continue

            if workingCopy.Private and not dlSvcRecord.GetConfiguration()["sync_private"]:
                logger.info("\t\t...is private and restricted from sync")  # Sync exclusion instead?
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.Private))
                continue
            try:
                workingCopy.CheckSanity()
            except:
                logger.info("\t\t...failed sanity check")
                self._accumulateExclusions(dlSvcRecord, APIExcludeActivity("Sanity check failed " + _formatExc(), activity=workingCopy))
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.SanityError))
                continue
            else:
                act = workingCopy
                act.SourceConnection = dlSvcRecord
                break  # succesfully got the activity + passed sanity checks, can stop now
        # If nothing was downloaded at this point, the activity record will show the most recent error - which is fine enough, since only one service is needed to get the activity.
        return act, dlSvc

    def _uploadActivity(self, activity, destinationServiceRec):
        destSvc = destinationServiceRec.Service
        try:
            return destSvc.UploadActivity(destinationServiceRec, activity)
        except (ServiceException, ServiceWarning) as e:
            self._syncErrors[destinationServiceRec._id].append(_packServiceException(SyncStep.Upload, e))
            if e.Block and e.Scope == ServiceExceptionScope.Service: # Similarly, no behaviour to immediately abort the sync if an account-level exception is raised
                self._excludeService(destinationServiceRec, e.UserException)
            if not issubclass(e.__class__, ServiceWarning):
                activity.Record.MarkAsNotPresentOn(destinationServiceRec, e.UserException)
                raise UploadException()
        except Exception as e:
            self._syncErrors[destinationServiceRec._id].append({"Step": SyncStep.Upload, "Message": _formatExc()})
            activity.Record.MarkAsNotPresentOn(destinationServiceRec, UserException(UserExceptionType.UploadError))
            raise UploadException()

    def Run(self, exhaustive=False, null_next_sync_on_unlock=False, heartbeat_callback=None):
        if len(self.user["ConnectedServices"]) <= 1:
            return # Done and done!
        from tapiriik.services.interchange import ActivityStatisticUnit

        # Mark this user as in-progress.
        self._lockUser()

        # Reset their progress
        self._updateSyncProgress(SyncStep.List, 0)

        self._initializeUserLogging()

        logger.info("Beginning sync for " + str(self.user["_id"]) + "(exhaustive: " + str(exhaustive) + ")")

        # Sets up serviceConnections
        self._loadServiceData()

        self._loadExtendedAuthData()

        self._activities = []
        self._excludedServices = {}
        self._deferredServices = []

        self._initializePersistedSyncErrorsAndExclusions()

        self._initializeActivityRecords()

        try:
            try:
                for conn in self._serviceConnections:
                    # If we're not going to be doing anything anyways, stop now
                    if len(self._serviceConnections) - len(self._excludedServices) <= 1:
                        raise SynchronizationCompleteException()

                    self._primeExtendedAuthDetails(conn)

                    logger.info("Ensuring partial sync poll subscription")
                    self._ensurePartialSyncPollingSubscription(conn)

                    if not exhaustive and conn.Service.PartialSyncRequiresTrigger and "TriggerPartialSync" not in conn.__dict__ and not conn.Service.ShouldForcePartialSyncTrigger(conn):
                        logger.info("Service %s has not been triggered" % conn.Service.ID)
                        self._deferredServices.append(conn._id)
                        continue

                    if heartbeat_callback:
                        heartbeat_callback(SyncStep.List)

                    self._updateSyncProgress(SyncStep.List, conn.Service.ID)
                    self._downloadActivityList(conn, exhaustive)

                self._applyFallbackTZ()

                self._processActivityOrigins()

                # Makes reading the logs much easier.
                self._activities = sorted(self._activities, key=lambda v: v.StartTime.replace(tzinfo=None), reverse=True)

                totalActivities = len(self._activities)
                processedActivities = 0

                for activity in self._activities:
                    logger.info(str(activity) + " " + str(activity.UID[:3]) + " from " + str([[y.Service.ID for y in self._serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()]))
                    logger.info(" Name: %s Notes: %s Distance: %s%s" % (activity.Name[:15] if activity.Name else "", activity.Notes[:15] if activity.Notes else "", activity.Stats.Distance.Value, activity.Stats.Distance.Units))
                    try:
                        activity.Record = self._findOrCreateActivityRecord(activity) # Make it a member of the activity, to avoid passing it around as a seperate parameter everywhere.

                        self._updateSynchronizedActivities(activity)
                        self._updateActivityRecordInitialPrescence(activity)

                        # We don't always know if the activity is private before it's downloaded, but we can check anyways since it saves a lot of time.
                        if activity.Private:
                            actAvailableFromConnIds = activity.ServiceDataCollection.keys()
                            actAvailableFromConns = [[x for x in self._serviceConnections if x._id == dlSvcRecId][0] for dlSvcRecId in actAvailableFromConnIds]
                            override_private = False
                            for conn in actAvailableFromConns:
                                if conn.GetConfiguration()["sync_private"]:
                                    override_private = True
                                    break

                            if not override_private:
                                logger.info("\t\t...is private and restricted from sync (pre-download)")  # Sync exclusion instead?
                                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.Private))
                                raise ActivityShouldNotSynchronizeException()

                        recipientServices = None
                        eligibleServices = None
                        while True:
                            # recipientServices are services that don't already have this activity
                            recipientServices = self._determineRecipientServices(activity)
                            if len(recipientServices) == 0:
                                totalActivities -= 1  # doesn't count
                                raise ActivityShouldNotSynchronizeException()

                            # eligibleServices are services that are permitted to receive this activity - taking into account flow exceptions, excluded services, unfufilled configuration requirements, etc.
                            eligibleServices = self._determineEligibleRecipientServices(activity=activity, recipientServices=recipientServices)

                            if not len(eligibleServices):
                                logger.info("\t\t...has no eligible destinations")
                                totalActivities -= 1  # Again, doesn't really count.
                                raise ActivityShouldNotSynchronizeException()

                            has_deferred = False
                            for conn in eligibleServices:
                                if conn._id in self._deferredServices:
                                    logger.info("Doing deferred list from %s" % conn.Service.ID)
                                    # no_add since...
                                    #  a) we're iterating over the list it'd be adding to, and who knows what will happen then
                                    #  b) for the current use of deferred services, we don't care about new activities
                                    self._downloadActivityList(conn, exhaustive, no_add=True)
                                    self._deferredServices.remove(conn._id)
                                    has_deferred = True

                            # If we had deferred listing activities from a service, we have to repeat this loop to consider the new info
                            # Otherwise, once was enough
                            if not has_deferred:
                                break


                        # This is after the above exit points since they're the most frequent (& cheapest) cases - want to avoid DB churn
                        if heartbeat_callback:
                            heartbeat_callback(SyncStep.Download)

                        if processedActivities == 0:
                            syncProgress = 0
                        elif totalActivities <= 0:
                            syncProgress = 1
                        else:
                            syncProgress = max(0, min(1, processedActivities / totalActivities))
                        self._updateSyncProgress(SyncStep.Download, syncProgress)

                        # The second most important line of logging in the application...
                        logger.info("\t\t...to " + str([x.Service.ID for x in recipientServices]))

                        # Download the full activity record
                        full_activity, activitySource = self._downloadActivity(activity)

                        if full_activity is None:  # couldn't download it from anywhere, or the places that had it said it was broken
                            # The activity record gets updated in _downloadActivity
                            processedActivities += 1  # we tried
                            raise ActivityShouldNotSynchronizeException()

                        full_activity.CleanStats()
                        full_activity.CleanWaypoints()

                        try:
                            full_activity.EnsureTZ()
                        except:
                            logger.error("\tCould not determine TZ")
                            self._accumulateExclusions(full_activity.SourceConnection, APIExcludeActivity("Could not determine TZ", activity=full_activity, permanent=False))
                            activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.UnknownTZ))
                            raise ActivityShouldNotSynchronizeException()
                        else:
                            logger.debug("\tDetermined TZ %s" % full_activity.TZ)

                        activity.Record.SetActivity(activity) # Update with whatever more accurate information we may have.

                        full_activity.Record = activity.Record # Some services don't return the same object, so this gets lost, which is meh, but...

                        for destinationSvcRecord in eligibleServices:
                            if heartbeat_callback:
                                heartbeat_callback(SyncStep.Upload)
                            destSvc = destinationSvcRecord.Service
                            if not destSvc.ReceivesStationaryActivities and full_activity.Stationary:
                                logger.info("\t\t...marked as stationary during download")
                                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.StationaryUnsupported))
                                continue

                            uploaded_external_id = None
                            logger.info("\t  Uploading to " + destSvc.ID)
                            try:
                                uploaded_external_id = self._uploadActivity(full_activity, destinationSvcRecord)
                            except UploadException:
                                continue # At this point it's already been added to the error collection, so we can just bail.
                            logger.info("\t  Uploaded")

                            activity.Record.MarkAsSynchronizedTo(destinationSvcRecord)

                            if uploaded_external_id:
                                # record external ID, for posterity (and later debugging)
                                db.uploaded_activities.insert({"ExternalID": uploaded_external_id, "Service": destSvc.ID, "UserExternalID": destinationSvcRecord.ExternalID, "Timestamp": datetime.utcnow()})
                            # flag as successful
                            db.connections.update({"_id": destinationSvcRecord._id},
                                                  {"$addToSet": {"SynchronizedActivities": {"$each": list(activity.UIDs)}}})

                            db.sync_stats.update({"ActivityID": activity.UID}, {"$addToSet": {"DestinationServices": destSvc.ID, "SourceServices": activitySource.ID}, "$set": {"Distance": activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value, "Timestamp": datetime.utcnow()}}, upsert=True)
                        del full_activity
                        processedActivities += 1
                    except ActivityShouldNotSynchronizeException:
                        continue
                    finally:
                        del activity

            except SynchronizationCompleteException:
                # This gets thrown when there is obviously nothing left to do - but we still need to clean things up.
                logger.info("SynchronizationCompleteException thrown")

            logger.info("Writing back service data")
            self._writeBackSyncErrorsAndExclusions()

            if exhaustive:
                # Clean up potentially orphaned records, since we know everything is here.
                logger.info("Clearing old activity records")
                self._dropUntouchedActivityRecords()

            logger.info("Writing back activity records")
            self._writeBackActivityRecords()

            logger.info("Finalizing")
            # Clear non-persisted extended auth details.
            self._destroyExtendedAuthData()
            # Unlock the user.
            self._unlockUser(null_next_sync_on_unlock)

        except SynchronizationConcurrencyException:
            raise # Don't spit out the "Core sync exception" error
        except:
            # oops.
            logger.exception("Core sync exception")
            raise
        else:
            logger.info("Finished sync for %s" % self.user["_id"])
        finally:
            self._closeUserLogging()


class UploadException(Exception):
    pass

class ActivityShouldNotSynchronizeException(Exception):
    pass

class SynchronizationCompleteException(Exception):
    pass


class SynchronizationConcurrencyException(Exception):
    pass


class SyncStep:
    List = "list"
    Download = "download"
    Upload = "upload"
