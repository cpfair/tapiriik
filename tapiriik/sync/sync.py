from tapiriik.database import db, cachedb, redis
from tapiriik.messagequeue import mq
from tapiriik.services import Service, ServiceRecord, APIExcludeActivity, ServiceException, ServiceExceptionScope, ServiceWarning, UserException, UserExceptionType
from tapiriik.settings import USER_SYNC_LOGS, DISABLED_SERVICES, WITHDRAWN_SERVICES
from .activity_record import ActivityRecord, ActivityServicePrescence
from datetime import datetime, timedelta
import sys
import os
import io
import socket
import traceback
import pprint
import copy
import random
import logging
import logging.handlers
import pymongo
import pytz
import kombu
import json
import bisect

# Set this up separate from the logger used in this scope, so services logging messages are caught and logged into user's files.
_global_logger = logging.getLogger("tapiriik")

_global_logger.setLevel(logging.DEBUG)

# In celery tasks, sys.stdout has already been monkeypatched
# So we'll assume they know what they're doing.
if hasattr(sys.stdout, "buffer"):
    logging_console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8'))
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

def _isWarning(exc):
    return issubclass(exc.__class__, ServiceWarning)

# It's practically an ORM!

def _packServiceException(step, e):
    res = {"Step": step, "Message": e.Message + "\n" + _formatExc(), "Block": e.Block, "Scope": e.Scope, "TriggerExhaustive": e.TriggerExhaustive, "Timestamp": datetime.utcnow()}
    if e.UserException:
        res["UserException"] = _packUserException(e.UserException)
    return res

def _packException(step):
    return {"Step": step, "Message": _formatExc(), "Timestamp": datetime.utcnow()}

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

    def InitializeWorkerBindings():
        Sync._channel = mq.channel()
        Sync._exchange = kombu.Exchange("tapiriik-users", type="direct")(Sync._channel)
        Sync._exchange.declare()
        Sync._global_queue = kombu.Queue("tapiriik-users")(Sync._channel)
        Sync._host_queue = kombu.Queue("tapiriik-users-%s" % socket.gethostname())(Sync._channel)
        Sync._global_queue.declare()
        Sync._host_queue.declare()
        # Bind to worker-specific and general routing keys
        Sync._global_queue.bind_to(exchange="tapiriik-users", routing_key="")
        Sync._host_queue.bind_to(exchange="tapiriik-users", routing_key=socket.gethostname())

    def PerformGlobalSync(heartbeat_callback=None, version=None, max_users=None):
        def _callback(body, message):
            Sync._consumeSyncTask(body, message, heartbeat_callback, version)

        Sync._consumer = kombu.Consumer(
            channel=Sync._channel,
            queues=[Sync._host_queue, Sync._global_queue],
            callbacks=[_callback],
            auto_declare=False
        )

        Sync._consumer.qos(prefetch_count=1, apply_global=False)

        Sync._consumer.consume()

        for _ in kombu.eventloop(mq, limit=max_users):
            pass

    def _consumeSyncTask(body, message, heartbeat_callback_direct, version):
        from tapiriik.auth import User

        user_id = body["user_id"]
        user = User.Get(user_id)
        if user is None:
            logger.warning("Could not find user %s - bailing" % user_id)
            message.ack() # Otherwise the entire thing grinds to a halt
            return
        if body["generation"] != user.get("QueuedGeneration", None):
            # QueuedGeneration being different means they've gone through sync_scheduler since this particular message was queued
            # So, discard this and wait for that message to surface
            # Should only happen when I manually requeue people
            logger.warning("Queue generation mismatch for %s - bailing" % user_id)
            message.ack()
            return

        def heartbeat_callback(state):
            heartbeat_callback_direct(state, user_id)

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
        if  ("ForcingExhaustiveSyncErrorCount" not in user and "NonblockingSyncErrorCount" in user and user["NonblockingSyncErrorCount"] > 0) or \
            ("ForcingExhaustiveSyncErrorCount" in user and user["ForcingExhaustiveSyncErrorCount"] > 0):
            exhaustive = True

        result = None
        try:
            if not user.get("BlockedOnBadActivitiesAcknowledgement", False):
                result = Sync.PerformUserSync(user, exhaustive, heartbeat_callback=heartbeat_callback)
        finally:
            nextSync = None
            if User.HasActivePayment(user):
                if User.GetConfiguration(user)["suppress_auto_sync"]:
                    logger.info("Not scheduling auto sync for paid user")
                else:
                    nextSync = datetime.utcnow() + Sync.SyncInterval + timedelta(seconds=random.randint(-Sync.SyncIntervalJitter.total_seconds(), Sync.SyncIntervalJitter.total_seconds()))
            if result and result.ForceNextSync:
                logger.info("Forcing next sync at %s" % result.ForceNextSync)
                nextSync = result.ForceNextSync
            reschedule_update = {
                "$set": {
                    "NextSynchronization": nextSync,
                    "LastSynchronization": datetime.utcnow(),
                    "LastSynchronizationVersion": version
                }, "$unset": {
                    "QueuedAt": None # Set by sync_scheduler when the record enters the MQ
                }
            }

            if result and result.ForceExhaustive:
                logger.info("Forcing next sync as exhaustive")
                reschedule_update["$set"]["NextSyncIsExhaustive"] = True
            else:
                reschedule_update["$unset"]["NextSyncIsExhaustive"] = ""

            scheduling_result = db.users.update(
                {
                    "_id": user["_id"]
                }, reschedule_update)
            reschedule_confirm_message = "User reschedule for %s returned %s" % (nextSync, scheduling_result)

            # Tack this on the end of the log file since otherwise it's lost for good (blegh, but nicer than moving logging out of the sync task?)
            user_log = open(USER_SYNC_LOGS + str(user["_id"]) + ".log", "a+")
            user_log.write("\n%s\n" % reschedule_confirm_message)
            user_log.close()

            logger.debug(reschedule_confirm_message)
            syncTime = (datetime.utcnow() - syncStart).total_seconds()
            db.sync_worker_stats.insert({"Timestamp": datetime.utcnow(), "Worker": os.getpid(), "Host": socket.gethostname(), "TimeTaken": syncTime})

        message.ack()

    def PerformUserSync(user, exhaustive=False, heartbeat_callback=None):
        return SynchronizationTask(user).Run(exhaustive=exhaustive, heartbeat_callback=heartbeat_callback)


class SynchronizationTask:
    _logFormat = '[%(levelname)-8s] %(asctime)s (%(name)s:%(lineno)d) %(message)s'
    _logDateFormat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, user):
        self.user = user

    def _lockUser(self):
        db.users.update({"_id": self.user["_id"]}, {"$set": {"SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname(), "SynchronizationStartTime": datetime.utcnow()}})

    def _unlockUser(self):
        unlock_result = db.users.update(
            {
                "_id": self.user["_id"]
            }, {
                "$unset": {
                    "SynchronizationWorker": None
                }
            })
        logger.debug("User unlock returned %s" % unlock_result)

    def _loadServiceData(self):
        self._connectedServiceIds = [x["ID"] for x in self.user["ConnectedServices"]]
        self._serviceConnections = [ServiceRecord(x) for x in db.connections.find({"_id": {"$in": self._connectedServiceIds}})]

    def _updateSyncProgress(self, step, progress):
        db.users.update({"_id": self.user["_id"]}, {"$set": {"SynchronizationProgress": progress, "SynchronizationStep": step}})

    def _initializeUserLogging(self):
        self._logging_file_handler = logging.handlers.RotatingFileHandler(USER_SYNC_LOGS + str(self.user["_id"]) + ".log", maxBytes=0, backupCount=5, encoding="utf-8")
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
        self._hasTransientSyncErrors = {}
        self._syncExclusions = {}

        for conn in self._serviceConnections:
            if hasattr(conn, "SyncErrors"):
                # Remove non-blocking errors
                self._syncErrors[conn._id] = [x for x in conn.SyncErrors if "Block" in x and x["Block"]]
                self._hasTransientSyncErrors[conn._id] = len(self._syncErrors[conn._id]) != len(conn.SyncErrors)
                del conn.SyncErrors
            else:
                self._syncErrors[conn._id] = []

            # Remove temporary exclusions (live tracking etc).
            self._syncExclusions[conn._id] = dict((k, v) for k, v in (conn.ExcludedActivities if conn.ExcludedActivities else {}).items() if v["Permanent"])

            if conn.ExcludedActivities:
                del conn.ExcludedActivities  # Otherwise the exception messages get really, really, really huge and break mongodb.

    def _writeBackSyncErrorsAndExclusions(self):
        nonblockingSyncErrorsCount = 0
        forcingExhaustiveSyncErrorsCount = 0
        blockingSyncErrorsCount = 0
        syncExclusionCount = 0
        for conn in self._serviceConnections:
            update_values = {
                "$set": {
                    "SyncErrors": self._syncErrors[conn._id],
                    "ExcludedActivities": self._syncExclusions[conn._id]
                }
            }

            if not self._isServiceExcluded(conn) and not self._shouldPersistServiceTrigger(conn):
                # Only reset the trigger if we succesfully got through the entire sync without bailing on this particular connection
                update_values["$unset"] = {"TriggerPartialSync": None, "TriggerPartialSyncPayloads": None}

            try:
                db.connections.update({"_id": conn._id}, update_values)
            except pymongo.errors.WriteError as e:
                if e.code == 17419: # Update makes document too large.
                    # Throw them all out - exhaustive sync will recover whichever still apply.
                    # NB we don't explicitly mark as exhaustive here, the error counts will trigger it if appropriate.
                    db.connections.update({"_id": conn._id}, {"$unset": {"SyncErrors": "", "ExcludedActivities": ""}})
                else:
                    raise
            nonblockingSyncErrorsCount += len([x for x in self._syncErrors[conn._id] if "Block" not in x or not x["Block"]])
            blockingSyncErrorsCount += len([x for x in self._syncErrors[conn._id] if "Block" in x and x["Block"]])
            forcingExhaustiveSyncErrorsCount += len([x for x in self._syncErrors[conn._id] if "Block" in x and x["Block"] and "TriggerExhaustive" in x and x["TriggerExhaustive"]])
            syncExclusionCount += len(self._syncExclusions[conn._id].items())

        db.users.update({"_id": self.user["_id"]}, {"$set": {"NonblockingSyncErrorCount": nonblockingSyncErrorsCount, "BlockingSyncErrorCount": blockingSyncErrorsCount, "ForcingExhaustiveSyncErrorCount": forcingExhaustiveSyncErrorsCount, "SyncExclusionCount": syncExclusionCount}})

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
                "Abscence": _activityPrescences(x.NotPresentOnServices),
                "FailureCounts": x.FailureCounts
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

    def _persistServiceTrigger(self, serviceRecord):
        self._persistTriggerServices[serviceRecord._id] = True

    def _shouldPersistServiceTrigger(self, serviceRecord):
        return serviceRecord._id in self._persistTriggerServices

    def _excludeService(self, serviceRecord, userException):
        self._excludedServices[serviceRecord._id] = userException if userException else None

    def _isServiceExcluded(self, serviceRecord):
        return serviceRecord._id in self._excludedServices

    def _getServiceExclusionUserException(self, serviceRecord):
        return self._excludedServices[serviceRecord._id]

    def _determineRecipientServices(self, activity):
        recipientServices = []
        for conn in self._serviceConnections:
            if not conn.Service.ReceivesActivities:
                # Nope.
                continue
            if conn._id in activity.ServiceDataCollection:
                # The activity record is updated earlier for these, blegh.
                continue
            elif hasattr(conn, "SynchronizedActivities") and len([x for x in activity.UIDs if x in conn.SynchronizedActivities]):
                continue
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
        activityStartTZOffsetLeeway = timedelta(minutes=1)
        timezoneErrorPeriod = timedelta(hours=38)
        from tapiriik.services.interchange import ActivityType
        for act in svcActivities:
            act.UIDs = set([act.UID])
            if not hasattr(act, "ServiceDataCollection"):
                act.ServiceDataCollection = {}
            if hasattr(act, "ServiceData") and act.ServiceData is not None:
                act.ServiceDataCollection[conn._id] = act.ServiceData
                del act.ServiceData
            else:
                act.ServiceDataCollection[conn._id] = None
            if act.TZ and not hasattr(act.TZ, "localize"):
                raise ValueError("Got activity with TZ type " + str(type(act.TZ)) + " instead of a pytz timezone")
            # Used to ensureTZ() right here - doubt it's needed any more?
            # Binsearch to find which activities actually need individual attention.
            # Otherwise it's O(mn^2).
            # self._activities is sorted most recent first
            relevantActivitiesStart = bisect.bisect_left(self._activities, act.StartTime + timezoneErrorPeriod)
            relevantActivitiesEnd = bisect.bisect_right(self._activities, act.StartTime - timezoneErrorPeriod, lo=relevantActivitiesStart)
            extantActIter = (
                              x for x in (self._activities[idx] for idx in range(relevantActivitiesStart, relevantActivitiesEnd)) if
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
                              )

            try:
                existingActivity = next(extantActIter)
            except StopIteration:
                existingActivity = None

            if existingActivity:
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
                existingActivity.Device = existingActivity.Device or act.Device
                if act.Stationary is not None:
                    if existingActivity.Stationary is None:
                        existingActivity.Stationary = act.Stationary
                    else:
                        existingActivity.Stationary = existingActivity.Stationary and act.Stationary # Let's be optimistic here
                else:
                    pass # Nothing to do - existElsewhere is either more speicifc or equivalently indeterminate
                if act.GPS is not None:
                    if existingActivity.GPS is None:
                        existingActivity.GPS = act.GPS
                    else:
                        existingActivity.GPS = act.GPS or existingActivity.GPS
                else:
                    pass # Similarly
                existingActivity.Stats.coalesceWith(act.Stats)

                serviceDataCollection = dict(act.ServiceDataCollection)
                serviceDataCollection.update(existingActivity.ServiceDataCollection)
                existingActivity.ServiceDataCollection = serviceDataCollection

                existingActivity.UIDs |= act.UIDs  # I think this is merited
                act.UIDs = existingActivity.UIDs  # stop the circular inclusion, not that it matters
                continue
            if not no_add:
                bisect.insort_left(self._activities, act)

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
            # ReceivesNonGPSActivitiesWithOtherSensorData doesn't matter if the activity is stationary.
            # (and the service accepts stationary activities - guaranteed immediately above)
            if not activity.Stationary:
                if not (destSvc.ReceivesNonGPSActivitiesWithOtherSensorData or activity.GPS is not False):
                    logger.info("\t\t" + destSvc.ID + " doesn't receive non-GPS activities")
                    activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.NonGPSUnsupported))
                    continue

            if activity.Record.GetFailureCount(destinationSvcRecord) >= destSvc.UploadRetryCount:
                logger.info("\t\t" + destSvc.ID + " has exceeded upload retry count")
                # There's already an error in the activity Record, no need to add anything more here
                continue

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
        if conn.Service.PartialSyncRequiresTrigger and conn.Service.PartialSyncTriggerRequiresSubscription and not conn.PartialSyncTriggerSubscribed:
            if conn.Service.RequiresExtendedAuthorizationDetails and not conn.ExtendedAuthorization:
                logger.info("No ext auth details, cannot subscribe")
                return # We (probably) can't subscribe unless we have their credentials. May need to change this down the road.
            try:
                conn.Service.SubscribeToPartialSyncTrigger(conn)
            except ServiceException as e:
                # Force sync as exhaustive until we're sure we're properly subscribed.
                self._sync_result.ForceExhaustive = True
                logger.exception("Failure while subscribing to partial sync trigger")

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

        if exhaustive and not svc.SupportsExhaustiveListing and not self._activities:
            # If we get to this point, we must already have activity listings from another service.
            logger.info("Account does not contain any services supporting exhaustive activity listing")
            self._excludeService(conn, UserException(UserExceptionType.Other))
            return

        if svc.RequiresExtendedAuthorizationDetails:
            if not conn.ExtendedAuthorization:
                logger.info("No extended auth details for " + svc.ID)
                self._excludeService(conn, UserException(UserExceptionType.MissingCredentials))
                return

        try:
            logger.info("\tRetrieving list from " + svc.ID)
            if not exhaustive or not self._activities:
                svcActivities, svcExclusions = svc.DownloadActivityList(conn, exhaustive)
            else:
                svcActivities, svcExclusions = svc.DownloadActivityList(conn, min((x.StartTime.replace(tzinfo=None) for x in self._activities)))
        except (ServiceException, ServiceWarning) as e:
            # Special-case rate limiting errors thrown during listing
            # Otherwise, things will melt down when the limit is reached
            # (lots of users will hit this error, then be marked for full synchronization later)
            # (but that's not really required)
            # Though we don't want to play with things if this exception needs to take the place of an earlier, more significant one
            #
            # I had previously removed this because I forgot that TriggerExhaustive defaults to true - this exception was *un*setting it
            # The issue prompting that change stemmed more from the fact that the rate-limiting errors were being marked as blocking,
            # ...not that they were getting marked as *not* triggering exhaustive synchronization

            if e.UserException and e.UserException.Type == UserExceptionType.RateLimited:
                e.TriggerExhaustive = conn._id in self._hasTransientSyncErrors and self._hasTransientSyncErrors[conn._id]
            self._syncErrors[conn._id].append(_packServiceException(SyncStep.List, e))
            self._excludeService(conn, e.UserException)
            if not _isWarning(e):
                return
        except Exception as e:
            self._syncErrors[conn._id].append(_packException(SyncStep.List))
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
            try:
                db.connections.update({"_id": {"$in": list(activity.ServiceDataCollection.keys())}},
                                      {"$addToSet": {"SynchronizedActivities": {"$each": list(activity.UIDs)}}},
                                      multi=True)
            except pymongo.errors.WriteError as e:
                if e.code == 17419: # Update makes document too large.
                    # Throw them all out - exhaustive sync will recover.
                    # I should probably check that this is actually due to transient issues - otherwise it'll keep happening.
                    db.connections.update({"_id": {"$in": list(activity.ServiceDataCollection.keys())}}, {"$unset": {"SynchronizedActivities": ""}})
                    self._sync_result.ForceExhaustive = True
                else:
                    raise

    def _updateActivityRecordInitialPrescence(self, activity):
        for connWithExistingActivityId in activity.ServiceDataCollection.keys():
            connWithExistingActivity = [x for x in self._serviceConnections if x._id == connWithExistingActivityId][0]
            activity.Record.MarkAsPresentOn(connWithExistingActivity)
        for conn in self._serviceConnections:
            if hasattr(conn, "SynchronizedActivities") and len([x for x in activity.UIDs if x in conn.SynchronizedActivities]):
                activity.Record.MarkAsPresentOn(conn)

    def _syncActivityRedisKey(user):
        return "recent-sync:%s" % user["_id"]

    def _pushRecentSyncActivity(self, activity, destinations):
        key = SynchronizationTask._syncActivityRedisKey(self.user)
        redis.lpush(key, json.dumps({"Name": activity.Name, "StartTime": activity.StartTime.isoformat(), "Type": activity.Type, "Timestamp": datetime.utcnow().isoformat(), "Destinations": destinations}))
        redis.ltrim(key, 0, 4) # Only keep 5

    def RecentSyncActivity(user):
        return [json.loads(x.decode("UTF-8")) for x in redis.lrange(SynchronizationTask._syncActivityRedisKey(user), 0, 4)]

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
            if not dlSvc.SuppliesActivities:
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.NoSupplier))
                logger.info("\t\t...does not supply activities")
                continue
            if activity.UID in self._syncExclusions[dlSvcRecord._id]:
                activity.Record.MarkAsNotPresentOtherwise(_unpackUserException(self._syncExclusions[dlSvcRecord._id][activity.UID]))
                logger.info("\t\t...has activity exclusion logged")
                continue
            if self._isServiceExcluded(dlSvcRecord):
                activity.Record.MarkAsNotPresentOtherwise(self._getServiceExclusionUserException(dlSvcRecord))
                logger.info("\t\t...service became excluded after listing") # Because otherwise we'd never have been trying to download from it in the first place.
                continue
            if activity.Record.GetFailureCount(dlSvcRecord) >= dlSvc.DownloadRetryCount:
                # We don't re-call MarkAsNotPresentOtherwise here
                # ...since its existing value will be the more illuminating as to the error
                # (and we can just check the failure count if we want to know if it's being ignored)
                logger.info("\t\t...download retry count exceeded")
                continue

            workingCopy = copy.copy(activity)  # we can hope
            # Load in the service data in the same place they left it.
            workingCopy.ServiceData = workingCopy.ServiceDataCollection[dlSvcRecord._id] if dlSvcRecord._id in workingCopy.ServiceDataCollection else None
            try:
                workingCopy = dlSvc.DownloadActivity(dlSvcRecord, workingCopy)
            except (ServiceException, ServiceWarning) as e:
                if not _isWarning(e):
                    # Persist the exception if we just exceeded the failure count
                    # (but not if a more useful blocking exception was provided)
                    activity.Record.IncrementFailureCount(dlSvcRecord)
                    if activity.Record.GetFailureCount(dlSvcRecord) >= dlSvc.DownloadRetryCount and not e.Block and (not e.UserException or e.UserException.Type != UserExceptionType.RateLimited):
                        e.Block = True
                        e.Scope = ServiceExceptionScope.Activity

                self._syncErrors[dlSvcRecord._id].append(_packServiceException(SyncStep.Download, e))

                if e.Block and e.Scope == ServiceExceptionScope.Service: # I can't imagine why the same would happen at the account level, so there's no behaviour to immediately abort the sync in that case.
                    self._excludeService(dlSvcRecord, e.UserException)
                if not _isWarning(e):
                    activity.Record.MarkAsNotPresentOtherwise(e.UserException)
                    continue
            except APIExcludeActivity as e:
                logger.info("\t\texcluded by service: %s" % e.Message)
                e.Activity = workingCopy
                self._accumulateExclusions(dlSvcRecord, e)
                activity.Record.MarkAsNotPresentOtherwise(e.UserException)
                continue
            except Exception as e:
                packed_exc = _packException(SyncStep.Download)

                activity.Record.IncrementFailureCount(dlSvcRecord)
                if activity.Record.GetFailureCount(dlSvcRecord) >= dlSvc.DownloadRetryCount:
                    # Blegh, should just make packServiceException work with this
                    packed_exc["Block"] = True
                    packed_exc["Scope"] = ServiceExceptionScope.Activity

                self._syncErrors[dlSvcRecord._id].append(packed_exc)
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.DownloadError))
                continue

            activity.Record.ResetFailureCount(dlSvcRecord)

            if workingCopy.Private and not dlSvcRecord.GetConfiguration()["sync_private"]:
                logger.info("\t\t...is private and restricted from sync")  # Sync exclusion instead?
                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.Private))
                continue
            try:
                workingCopy.CheckSanity()
            except:
                logger.info("\t\t...failed sanity check")
                self._accumulateExclusions(dlSvcRecord, APIExcludeActivity("Sanity check failed " + _formatExc(), activity=workingCopy, user_exception=UserException(UserExceptionType.SanityError)))
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
            if not _isWarning(e):
                activity.Record.IncrementFailureCount(destinationServiceRec)
                # The rate-limiting special case here is so that users don't get stranded due to rate limiting issues outside of their control
                if activity.Record.GetFailureCount(destinationServiceRec) >= destSvc.UploadRetryCount and not e.Block and (not e.UserException or e.UserException.Type != UserExceptionType.RateLimited):
                    e.Block = True
                    e.Scope = ServiceExceptionScope.Activity

            self._syncErrors[destinationServiceRec._id].append(_packServiceException(SyncStep.Upload, e))

            if e.Block and e.Scope == ServiceExceptionScope.Service: # Similarly, no behaviour to immediately abort the sync if an account-level exception is raised
                self._excludeService(destinationServiceRec, e.UserException)
            if not _isWarning(e):
                activity.Record.MarkAsNotPresentOn(destinationServiceRec, e.UserException if e.UserException else UserException(UserExceptionType.UploadError))
                raise UploadException()
        except Exception as e:
            packed_exc = _packException(SyncStep.Upload)

            activity.Record.IncrementFailureCount(destinationServiceRec)
            if activity.Record.GetFailureCount(destinationServiceRec) >= destSvc.UploadRetryCount:
                packed_exc["Block"] = True
                packed_exc["Scope"] = ServiceExceptionScope.Activity

            self._syncErrors[destinationServiceRec._id].append(packed_exc)
            activity.Record.MarkAsNotPresentOn(destinationServiceRec, UserException(UserExceptionType.UploadError))
            raise UploadException()

        activity.Record.ResetFailureCount(destinationServiceRec)

    def Run(self, exhaustive=False, null_next_sync_on_unlock=False, heartbeat_callback=None):
        from tapiriik.auth import User
        from tapiriik.services.interchange import ActivityStatisticUnit

        if len(self.user["ConnectedServices"]) <= 1:
            return # Done and done!

        sync_result = SynchronizationTaskResult()
        self._sync_result = sync_result

        self._user_config = User.GetConfiguration(self.user)

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
        self._persistTriggerServices = {}

        self._initializePersistedSyncErrorsAndExclusions()

        self._initializeActivityRecords()

        try:
            try:
                # Sort services that don't support exhaustive listing last.
                # That way, we can provide them with the proper bounds for listing based
                # on activities from other services.
                for conn in sorted(self._serviceConnections,
                                   key=lambda x: x.Service.SupportsExhaustiveListing,
                                   reverse=True):
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

                    if not conn.Service.SuppliesActivities:
                        logger.info("Service %s does not supply activities - deferring listing till first upload" % conn.Service.ID)
                        self._deferredServices.append(conn._id)
                        continue

                    if heartbeat_callback:
                        heartbeat_callback(SyncStep.List)

                    self._updateSyncProgress(SyncStep.List, conn.Service.ID)
                    self._downloadActivityList(conn, exhaustive)

                self._applyFallbackTZ()

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

                        actAvailableFromConnIds = activity.ServiceDataCollection.keys()
                        actAvailableFromConns = [[x for x in self._serviceConnections if x._id == dlSvcRecId][0] for dlSvcRecId in actAvailableFromConnIds]

                        # Check if this is too soon to synchronize
                        if self._user_config["sync_upload_delay"]:
                            endtime = activity.EndTime
                            tz = endtime.tzinfo
                            if not tz and activity.FallbackTZ:
                                tz = activity.FallbackTZ
                                endtime = tz.localize(endtime)

                            if tz and endtime: # We can't really know for sure otherwise
                                time_past = (datetime.utcnow() - endtime.astimezone(pytz.utc).replace(tzinfo=None))
                                 # I believe astimezone(utc) is scrubbing the DST away - put it back here.
                                 # We must try this twice because not all of our TZ objects are pytz for... some reason.
                                 # And, thus, dst() may not accept is_dst.

                                try:
                                    dst_offset = tz.dst(endtime.replace(tzinfo=None))
                                except pytz.AmbiguousTimeError:
                                    dst_offset = tz.dst(endtime.replace(tzinfo=None), is_dst=False)

                                if dst_offset:
                                    time_past += dst_offset

                                time_remaining = timedelta(seconds=self._user_config["sync_upload_delay"]) - time_past
                                logger.debug(" %s since upload" % time_past)
                                if time_remaining > timedelta(0):
                                    activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.Deferred))
                                    # Only reschedule if it won't slow down their auto-sync timing
                                    if time_remaining < (Sync.SyncInterval + Sync.SyncIntervalJitter):
                                        next_sync = datetime.utcnow() + time_remaining
                                        # Reschedule them so this activity syncs immediately on schedule
                                        sync_result.ForceScheduleNextSyncOnOrBefore(next_sync)

                                    logger.info("\t\t...is delayed for %s (out of %s)" % (time_remaining, timedelta(seconds=self._user_config["sync_upload_delay"])))
                                    # We need to ensure we check these again when the sync re-runs
                                    for conn in actAvailableFromConns:
                                        self._persistServiceTrigger(conn)
                                    raise ActivityShouldNotSynchronizeException()

                        if self._user_config["sync_skip_before"]:
                            if activity.StartTime.replace(tzinfo=None) < self._user_config["sync_skip_before"]:
                                logger.info("\t\t...predates configured sync window")
                                activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.PredatesWindow))
                                raise ActivityShouldNotSynchronizeException()

                        # We don't always know if the activity is private before it's downloaded, but we can check anyways since it saves a lot of time.
                        if activity.Private:
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
                        except Exception as e:
                            logger.error("\tCould not determine TZ %s" % e)
                            self._accumulateExclusions(full_activity.SourceConnection, APIExcludeActivity("Could not determine TZ", activity=full_activity, permanent=False))
                            activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.UnknownTZ))
                            raise ActivityShouldNotSynchronizeException()
                        else:
                            logger.debug("\tDetermined TZ %s" % full_activity.TZ)

                        try:
                            full_activity.CheckTimestampSanity()
                        except ValueError as e:
                            logger.warning("\t\t...failed timestamp sanity check - %s" % e)
                            # self._accumulateExclusions(full_activity.SourceConnection, APIExcludeActivity("Timestamp sanity check failed", activity=full_activity, permanent=True))
                            # activity.Record.MarkAsNotPresentOtherwise(UserException(UserExceptionType.SanityError))
                            # raise ActivityShouldNotSynchronizeException()

                        activity.Record.SetActivity(activity) # Update with whatever more accurate information we may have.

                        full_activity.Record = activity.Record # Some services don't return the same object, so this gets lost, which is meh, but...

                        successful_destination_service_ids = []

                        for destinationSvcRecord in eligibleServices:
                            if heartbeat_callback:
                                heartbeat_callback(SyncStep.Upload)
                            destSvc = destinationSvcRecord.Service
                            if not destSvc.ReceivesStationaryActivities and full_activity.Stationary:
                                logger.info("\t\t...marked as stationary during download")
                                activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.StationaryUnsupported))
                                continue
                            if not full_activity.Stationary:
                                if not (destSvc.ReceivesNonGPSActivitiesWithOtherSensorData or full_activity.GPS):
                                    logger.info("\t\t...marked as non-GPS during download")
                                    activity.Record.MarkAsNotPresentOn(destinationSvcRecord, UserException(UserExceptionType.NonGPSUnsupported))
                                    continue

                            uploaded_external_id = None
                            logger.info("\t  Uploading to " + destSvc.ID)
                            try:
                                uploaded_external_id = self._uploadActivity(full_activity, destinationSvcRecord)
                            except UploadException:
                                continue # At this point it's already been added to the error collection, so we can just bail.
                            logger.info("\t  Uploaded")

                            activity.Record.MarkAsSynchronizedTo(destinationSvcRecord)
                            successful_destination_service_ids.append(destSvc.ID)

                            if uploaded_external_id:
                                # record external ID, for posterity (and later debugging)
                                db.uploaded_activities.insert({"ExternalID": uploaded_external_id, "Service": destSvc.ID, "UserExternalID": destinationSvcRecord.ExternalID, "Timestamp": datetime.utcnow()})
                            # flag as successful
                            db.connections.update({"_id": destinationSvcRecord._id},
                                                  {"$addToSet": {"SynchronizedActivities": {"$each": list(activity.UIDs)}}})

                            db.sync_stats.update({"ActivityID": activity.UID}, {"$addToSet": {"DestinationServices": destSvc.ID, "SourceServices": activitySource.ID}, "$set": {"Distance": activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value, "Timestamp": datetime.utcnow()}}, upsert=True)

                        if len(successful_destination_service_ids):
                            self._pushRecentSyncActivity(full_activity, successful_destination_service_ids)
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

            logger.info("Unlocking user")
            # Unlock the user.
            self._unlockUser()

        except:
            # oops.
            logger.exception("Core sync exception")
            raise
        else:
            logger.info("Finished sync for %s (worker %d)" % (self.user["_id"], os.getpid()))
        finally:
            self._closeUserLogging()

        return sync_result


class SynchronizationTaskResult:
    def __init__(self, force_next_sync=None, force_exhaustive=False):
        self.ForceNextSync = force_next_sync
        self.ForceExhaustive = force_exhaustive

    def ForceScheduleNextSyncOnOrBefore(self, next_sync):
        self.ForceNextSync = self.ForceNextSync if self.ForceNextSync and self.ForceNextSync < next_sync else next_sync


class UploadException(Exception):
    pass

class ActivityShouldNotSynchronizeException(Exception):
    pass

class SynchronizationCompleteException(Exception):
    pass

class SyncStep:
    List = "list"
    Download = "download"
    Upload = "upload"
