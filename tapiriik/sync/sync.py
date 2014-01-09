from tapiriik.database import db, cachedb
from tapiriik.services import Service, ServiceRecord, APIExcludeActivity, ServiceException, ServiceExceptionScope, ServiceWarning
from tapiriik.settings import USER_SYNC_LOGS, DISABLED_SERVICES, WITHDRAWN_SERVICES
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

def _packServiceException(step, e):
    res = {"Step": step, "Message": e.Message + "\n" + _formatExc(), "Block": e.Block, "Scope": e.Scope}
    if e.UserException:
        res["UserException"] = {"Type": e.UserException.Type, "Extra": e.UserException.Extra, "InterventionRequired": e.UserException.InterventionRequired, "ClearGroup": e.UserException.ClearGroup}
    return res


class Sync:

    SyncInterval = timedelta(hours=1)
    SyncIntervalJitter = timedelta(minutes=5)
    MinimumSyncInterval = timedelta(seconds=30)
    MaximumIntervalBeforeExhaustiveSync = timedelta(days=14)  # Based on the general page size of 50 activites, this would be >3/day...

    _logFormat = '[%(levelname)-8s] %(asctime)s (%(name)s:%(lineno)d) %(message)s'
    _logDateFormat = '%Y-%m-%d %H:%M:%S'

    def ScheduleImmediateSync(user, exhaustive=None):
        if exhaustive is None:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})
        else:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow(), "NextSyncIsExhaustive": exhaustive}})

    def SetNextSyncIsExhaustive(user, exhaustive=False):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSyncIsExhaustive": exhaustive}})

    def _determineRecipientServices(activity, allConnections):
        recipientServices = []
        for conn in allConnections:
            if activity.Type not in conn.Service.SupportedActivities:
                logger.debug("\t...%s doesn't support type %s" % (conn.Service.ID, activity.Type))
            elif hasattr(conn, "SynchronizedActivities") and len([x for x in activity.UIDs if x in conn.SynchronizedActivities]):
                pass
            elif conn._id in activity.ServiceDataCollection:
                pass
            else:
                recipientServices.append(conn)
        return recipientServices

    def _fromSameService(activityA, activityB):
        otherSvcIds = activityB.ServiceDataCollection.keys()
        for uploadAId in activityA.ServiceDataCollection.keys():
            if uploadAId in otherSvcIds:
                return True
        return False

    def _coalesceDatetime(a, b, knownTz=None):
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

    def _accumulateActivities(conn, svcActivities, activityList):
        # Yep, abs() works on timedeltas
        activityStartLeeway = timedelta(minutes=3)
        activityStartTZOffsetLeeway = timedelta(seconds=10)
        timezoneErrorPeriod = timedelta(hours=38)
        from tapiriik.services.interchange import ActivityType
        for act in svcActivities:
            act.UIDs = [act.UID]
            if not hasattr(act, "ServiceDataCollection"):
                act.ServiceDataCollection = {}
            if hasattr(act, "ServiceData") and act.ServiceData is not None:
                act.ServiceDataCollection[conn._id] = act.ServiceData
                del act.ServiceData
            if act.TZ and not hasattr(act.TZ, "localize"):
                raise ValueError("Got activity with TZ type " + str(type(act.TZ)) + " instead of a pytz timezone")
            # Used to ensureTZ() right here - doubt it's needed any more?
            existElsewhere = [x for x in activityList if x.UID == act.UID
                              or  # check to see if the activities are reasonably close together to be considered duplicate
                              (x.StartTime is not None and
                               act.StartTime is not None and
                               (act.StartTime.tzinfo is not None) == (x.StartTime.tzinfo is not None) and
                               abs(act.StartTime-x.StartTime) < activityStartLeeway
                              )
                              or  # try comparing the time as if it were TZ-aware and in the expected TZ (this won't actually change the value of the times being compared)
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
                              ]
            if len(existElsewhere) > 0:
                existingActivity = existElsewhere[0]
                # we don't merge the exclude values here, since at this stage the services have the option of just not returning those activities
                if act.TZ is not None and existingActivity.TZ is None:
                    existingActivity.TZ = act.TZ
                    existingActivity.DefineTZ()
                existingActivity.FallbackTZ = existingActivity.FallbackTZ if existingActivity.FallbackTZ else act.FallbackTZ
                # tortuous merging logic is tortuous
                existingActivity.StartTime = Sync._coalesceDatetime(existingActivity.StartTime, act.StartTime)
                existingActivity.EndTime = Sync._coalesceDatetime(existingActivity.EndTime, act.EndTime, knownTz=existingActivity.StartTime.tzinfo)
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

                existingActivity.UIDs += act.UIDs  # I think this is merited
                act.UIDs = existingActivity.UIDs  # stop the circular inclusion, not that it matters
                continue
            activityList.append(act)

    def _determineEligibleRecipientServices(activity, connectedServices, recipientServices, excludedServices, user):
        from tapiriik.auth import User
        eligibleServices = []
        for destinationSvcRecord in recipientServices:
            if destinationSvcRecord in excludedServices:
                logger.info("\t\tExcluded " + destinationSvcRecord.Service.ID)
                continue  # we don't know for sure if it needs to be uploaded, hold off for now
            flowException = False

            sources = [[y for y in connectedServices if y._id == x][0] for x in activity.ServiceDataCollection.keys()]
            if hasattr(activity, "Origin") and "SkipOriginCheck" not in user:
                sources = [activity.Origin]
            for src in sources:
                if User.CheckFlowException(user, src, destinationSvcRecord):
                    flowException = True
                    break
            # This isn't an absolute failure - it's possible we could still take an indirect route around this exception
            # But only if they've allowed it
            if flowException:
                # Eventual destinations, since it'd eventually be synced from these anyways
                secondLevelSources = [x for x in recipientServices if x != destinationSvcRecord]
                # Other places this activity exists - the alternate routes
                secondLevelSources += [[y for y in connectedServices if y._id == x][0] for x in activity.ServiceDataCollection.keys()]
                for secondLevelSrc in secondLevelSources:
                    if (secondLevelSrc.GetConfiguration()["allow_activity_flow_exception_bypass_via_self"] or "SkipOriginCheck" in user) and not User.CheckFlowException(user, secondLevelSrc, destinationSvcRecord):
                        flowException = False
                        break

            if flowException:
                logger.info("\t\tFlow exception for " + destinationSvcRecord.Service.ID)
                continue
            destSvc = destinationSvcRecord.Service
            if destSvc.RequiresConfiguration(destinationSvcRecord):
                logger.info("\t\t" + destSvc.ID + " not configured")
                continue  # not configured, so we won't even try
            if not destSvc.ReceivesStationaryActivities and activity.Stationary:
                logger.info("\t\t" + destSvc.ID + " doesn't receive stationary activities")
                continue # Missing this originally, no wonder...
            eligibleServices.append(destinationSvcRecord)
        return eligibleServices

    def _accumulateExclusions(serviceRecord, exclusions, tempSyncExclusions):
        if type(exclusions) is not list:
            exclusions = [exclusions]
        for exclusion in exclusions:
            identifier = exclusion.Activity.UID if exclusion.Activity else exclusion.ExternalActivityID
            if not identifier:
                raise ValueError("Activity excluded with no identifying information")
            identifier = str(identifier).replace(".", "_")
            tempSyncExclusions[serviceRecord._id][identifier] = {"Message": exclusion.Message, "Activity": str(exclusion.Activity) if exclusion.Activity else None, "ExternalActivityID": exclusion.ExternalActivityID, "Permanent": exclusion.Permanent, "Effective": datetime.utcnow()}

    def _estimateFallbackTZ(activities):
        from collections import Counter
        # With the hope that the majority of the activity records returned will have TZs, and the user's current TZ will constitute the majority.
        TZOffsets = [x.StartTime.utcoffset().total_seconds() / 60 for x in activities if x.TZ is not None]
        mode = Counter(TZOffsets).most_common(1)
        if not len(mode):
            return None
        return pytz.FixedOffset(mode[0][0])

    def PerformGlobalSync(heartbeat_callback=None):
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
                db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": nextSync, "LastSynchronization": datetime.utcnow()}, "$unset": {"NextSyncIsExhaustive": None}})
                syncTime = (datetime.utcnow() - syncStart).total_seconds()
                db.sync_worker_stats.insert({"Timestamp": datetime.utcnow(), "Worker": os.getpid(), "Host": socket.gethostname(), "TimeTaken": syncTime})
        return userCt

    def PerformUserSync(user, exhaustive=False, null_next_sync_on_unlock=False, heartbeat_callback=None):
        from tapiriik.services.interchange import ActivityStatisticUnit
        # And thus begins the monolithic sync function that's a pain to test.
        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]

        if len(connectedServiceIds) <= 1:
            return  # nothing's going anywhere anyways

        # mark this user as in-progress
        db.users.update({"_id": user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname(), "SynchronizationProgress": 0, "SynchronizationStartTime": datetime.utcnow()}})
        lockCheck = db.users.find_one({"_id": user["_id"], "SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

        def _updateSyncProgress(step, progress):
            db.users.update({"_id": user["_id"]}, {"$set": {"SynchronizationProgress": progress, "SynchronizationStep": step}})

        logging_file_handler = logging.handlers.RotatingFileHandler(USER_SYNC_LOGS + str(user["_id"]) + ".log", maxBytes=5242880, backupCount=1)
        logging_file_handler.setFormatter(logging.Formatter(Sync._logFormat, Sync._logDateFormat))
        _global_logger.addHandler(logging_file_handler)

        logger.info("Beginning sync for " + str(user["_id"]) + "(exhaustive: " + str(exhaustive) + ")")
        try:
            serviceConnections = [ServiceRecord(x) for x in db.connections.find({"_id": {"$in": connectedServiceIds}})]
            allExtendedAuthDetails = list(cachedb.extendedAuthDetails.find({"ID": {"$in": connectedServiceIds}}))
            activities = []

            excludedServices = []

            tempSyncErrors = {}
            tempSyncExclusions = {}

            for conn in serviceConnections:

                svc = conn.Service

                if hasattr(conn, "SyncErrors"):
                    # Remove non-blocking errors
                    tempSyncErrors[conn._id] = [x for x in conn.SyncErrors if "Block" in x and x["Block"]]
                    del conn.SyncErrors
                else:
                    tempSyncErrors[conn._id] = []

                # Remove temporary exclusions (live tracking etc).
                tempSyncExclusions[conn._id] = dict((k, v) for k, v in (conn.ExcludedActivities if conn.ExcludedActivities else {}).items() if v["Permanent"])

                if conn.ExcludedActivities:
                    del conn.ExcludedActivities  # Otherwise the exception messages get really, really, really huge and break mongodb.

                # If we're not going to be doing anything anyways, stop now
                if len(serviceConnections) - len(excludedServices) <= 1:
                    activities = []
                    break

                if heartbeat_callback:
                    heartbeat_callback(SyncStep.List)

                # Bail out as appropriate for the entire account (tempSyncErrors contains only blocking errors at this point)
                if [x for x in tempSyncErrors[conn._id] if x["Scope"] == ServiceExceptionScope.Account]:
                    activities = [] # Kinda meh, I'll make it better when I break this into seperate functions, whenever that happens...
                    break

                # ...and for this specific service
                if [x for x in tempSyncErrors[conn._id] if x["Scope"] == ServiceExceptionScope.Service]:
                    logger.info("Service %s is blocked:" % conn.Service.ID)
                    excludedServices.append(conn)
                    continue

                if svc.ID in DISABLED_SERVICES or svc.ID in WITHDRAWN_SERVICES:
                    logger.info("Service %s is widthdrawn" % conn.Service.ID)
                    excludedServices.append(conn)
                    continue

                if svc.RequiresExtendedAuthorizationDetails:
                    if not hasattr(conn, "ExtendedAuthorization") or not conn.ExtendedAuthorization:
                        extAuthDetails = [x["ExtendedAuthorization"] for x in allExtendedAuthDetails if x["ID"] == conn._id]
                        if not len(extAuthDetails):
                            logger.info("No extended auth details for " + svc.ID)
                            excludedServices.append(conn)
                            continue
                        # the connection never gets saved in full again, so we can sub these in here at no risk
                        conn.ExtendedAuthorization = extAuthDetails[0]

                _updateSyncProgress(SyncStep.List, svc.ID)
                try:
                    logger.info("\tRetrieving list from " + svc.ID)
                    svcActivities, svcExclusions = svc.DownloadActivityList(conn, exhaustive)
                except (ServiceException, ServiceWarning) as e:
                    tempSyncErrors[conn._id].append(_packServiceException(SyncStep.List, e))
                    excludedServices.append(conn)
                    if not issubclass(e.__class__, ServiceWarning):
                        continue
                except Exception as e:
                    tempSyncErrors[conn._id].append({"Step": SyncStep.List, "Message": _formatExc()})
                    excludedServices.append(conn)
                    continue
                Sync._accumulateExclusions(conn, svcExclusions, tempSyncExclusions)
                Sync._accumulateActivities(conn, svcActivities, activities)

            # Attempt to assign fallback TZs to all stationary/potentially-stationary activities, since we may not be able to determine TZ any other way.
            fallbackTZ = Sync._estimateFallbackTZ(activities)
            if fallbackTZ:
                logger.info("Setting fallback TZs to %s" % fallbackTZ )
                for act in activities:
                    act.FallbackTZ = fallbackTZ

            logger.info("Reading activity origins")
            origins = list(db.activity_origins.find({"ActivityUID": {"$in": [x.UID for x in activities]}}))
            activitiesWithOrigins = [x["ActivityUID"] for x in origins]

            # Makes reading the logs much easier.
            activities = sorted(activities, key=lambda v: v.StartTime.replace(tzinfo=None), reverse=True)

            logger.info("Populating origins")
            # Populate origins
            for activity in activities:
                if len(activity.ServiceDataCollection.keys()) == 1:
                    if not len(excludedServices):  # otherwise it could be incorrectly recorded
                        # we can log the origin of this activity
                        if activity.UID not in activitiesWithOrigins:  # No need to hammer the database updating these when they haven't changed
                            logger.info("\t\t Updating db with origin for proceeding activity")
                            db.activity_origins.insert({"ActivityUID": activity.UID, "Origin": {"Service": [[y for y in serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0].Service.ID, "ExternalID": [[y.ExternalID for y in serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0]}})
                        activity.Origin = [[y for y in serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()][0]
                else:
                    if activity.UID in activitiesWithOrigins:
                        knownOrigin = [x for x in origins if x["ActivityUID"] == activity.UID]
                        connectedOrigins = [x for x in serviceConnections if knownOrigin[0]["Origin"]["Service"] == x.Service.ID and knownOrigin[0]["Origin"]["ExternalID"] == x.ExternalID]
                        if len(connectedOrigins) > 0:  # they might have disconnected it
                            activity.Origin = connectedOrigins[0]
                        else:
                            activity.Origin = ServiceRecord(knownOrigin[0]["Origin"])  # I have it on good authority that this will work

            totalActivities = len(activities)
            processedActivities = 0

            for activity in activities:
                logger.info(str(activity) + " " + str(activity.UID[:3]) + " from " + str([[y.Service.ID for y in serviceConnections if y._id == x][0] for x in activity.ServiceDataCollection.keys()]))
                logger.info(" Name: %s Notes: %s Distance: %s%s" % (activity.Name[:15] if activity.Name else "", activity.Notes[:15] if activity.Notes else "", activity.Stats.Distance.Value, activity.Stats.Distance.Units))
                # Locally mark this activity as present on the appropriate services.
                # These needs to happen regardless of whether the activity is going to be synchronized.
                #   Before, I had moved this under all the eligibility/recipient checks, but that could cause persistent duplicate activities when the user had already manually uploaded the same activity to multiple sites.
                updateServicesWithExistingActivity = False
                for serviceWithExistingActivityId in activity.ServiceDataCollection.keys():
                    serviceWithExistingActivity = [x for x in serviceConnections if x._id == serviceWithExistingActivityId][0]
                    if not hasattr(serviceWithExistingActivity, "SynchronizedActivities") or not (set(activity.UIDs) <= set(serviceWithExistingActivity.SynchronizedActivities)):
                        updateServicesWithExistingActivity = True
                        break
                if updateServicesWithExistingActivity:
                    logger.debug("\t\tUpdating SynchronizedActivities")
                    db.connections.update({"_id": {"$in": list(activity.ServiceDataCollection.keys())}},
                                          {"$addToSet": {"SynchronizedActivities": {"$each": activity.UIDs}}},
                                          multi=True)

                # We don't always know if the activity is private before it's downloaded, but we can check anyways since it saves a lot of time.
                if activity.Private:
                    logger.info("\t\t...is private and restricted from sync (pre-download)")  # Sync exclusion instead?
                    del activity
                    continue

                # recipientServices are services that don't already have this activity
                recipientServices = Sync._determineRecipientServices(activity, serviceConnections)
                if len(recipientServices) == 0:
                    totalActivities -= 1  # doesn't count
                    del activity
                    continue

                # eligibleServices are services that are permitted to receive this activity - taking into account flow exceptions, excluded services, unfufilled configuration requirements, etc.
                eligibleServices = Sync._determineEligibleRecipientServices(activity=activity, connectedServices=serviceConnections, recipientServices=recipientServices, excludedServices=excludedServices, user=user)

                if not len(eligibleServices):
                    logger.info("\t\t...has no eligible destinations")
                    totalActivities -= 1  # Again, doesn't really count.
                    del activity
                    continue

                # This is after the above exit points since they're the most frequent (& cheapest) cases - want to avoid DB churn
                if heartbeat_callback:
                    heartbeat_callback(SyncStep.Download)

                if processedActivities == 0:
                    syncProgress = 0
                elif totalActivities <= 0:
                    syncProgress = 1
                else:
                    syncProgress = max(0, min(1, processedActivities / totalActivities))
                _updateSyncProgress(SyncStep.Download, syncProgress)

                # The second most important line of logging in the application...
                logger.info("\t\t...to " + str([x.Service.ID for x in recipientServices]))

                # Download the full activity record
                act = None
                actAvailableFromSvcIds = activity.ServiceDataCollection.keys()
                actAvailableFromSvcs = [[x for x in serviceConnections if x._id == dlSvcRecId][0] for dlSvcRecId in actAvailableFromSvcIds]

                servicePriorityList = Service.PreferredDownloadPriorityList()
                actAvailableFromSvcs.sort(key=lambda x: servicePriorityList.index(x.Service))

                # TODO: redo this, it was completely broken:
                # Prefer retrieving the activity from its original source.

                for dlSvcRecord in actAvailableFromSvcs:
                    dlSvc = dlSvcRecord.Service
                    logger.info("\tfrom " + dlSvc.ID)
                    if activity.UID in tempSyncExclusions[dlSvcRecord._id]:
                        logger.info("\t\t...has activity exclusion logged")
                        continue
                    if dlSvcRecord in excludedServices:
                        logger.info("\t\t...service became excluded after listing") # Because otherwise we'd never have been trying to download from it in the first place.
                        continue

                    workingCopy = copy.copy(activity)  # we can hope
                    # Load in the service data in the same place they left it.
                    workingCopy.ServiceData = workingCopy.ServiceDataCollection[dlSvcRecord._id] if dlSvcRecord._id in workingCopy.ServiceDataCollection else None
                    try:
                        workingCopy = dlSvc.DownloadActivity(dlSvcRecord, workingCopy)
                    except (ServiceException, ServiceWarning) as e:
                        tempSyncErrors[dlSvcRecord._id].append(_packServiceException(SyncStep.Download, e))
                        if e.Block and e.Scope == ServiceExceptionScope.Service: # I can't imagine why the same would happen at the account level, so there's no behaviour to immediately abort the sync in that case.
                            excludedServices.append(dlSvcRecord)
                        if not issubclass(e.__class__, ServiceWarning):
                            continue
                    except APIExcludeActivity as e:
                        logger.info("\t\texcluded by service: %s" % e.Message)
                        e.Activity = workingCopy
                        Sync._accumulateExclusions(dlSvcRecord, e, tempSyncExclusions)
                        continue
                    except Exception as e:
                        tempSyncErrors[dlSvcRecord._id].append({"Step": SyncStep.Download, "Message": _formatExc()})
                        continue
                    finally:
                        # Clear this data now that we're done with it - it should never get used again
                        activity.ServiceDataCollection[dlSvcRecord._id] = None


                    if workingCopy.Private and not dlSvcRecord.GetConfiguration()["sync_private"]:
                        logger.info("\t\t...is private and restricted from sync")  # Sync exclusion instead?
                        continue
                    try:
                        workingCopy.CheckSanity()
                    except:
                        logger.info("\t\t...failed sanity check")
                        Sync._accumulateExclusions(dlSvcRecord, APIExcludeActivity("Sanity check failed " + _formatExc(), activity=workingCopy), tempSyncExclusions)
                        continue
                    else:
                        act = workingCopy
                        act.SourceConnection = dlSvcRecord
                        break  # succesfully got the activity + passed sanity checks, can stop now

                if act is None:  # couldn't download it from anywhere, or the places that had it said it was broken
                    processedActivities += 1  # we tried
                    del act
                    del activity
                    continue

                act.CleanStats()

                for destinationSvcRecord in eligibleServices:
                    if heartbeat_callback:
                        heartbeat_callback(SyncStep.Upload)
                    destSvc = destinationSvcRecord.Service
                    if not destSvc.ReceivesStationaryActivities and act.Stationary:
                        logger.info("\t\t...marked as stationary during download")
                        continue
                    try:
                        logger.info("\t  Uploading to " + destSvc.ID)
                        destSvc.UploadActivity(destinationSvcRecord, act)
                    except (ServiceException, ServiceWarning) as e:
                        tempSyncErrors[destinationSvcRecord._id].append(_packServiceException(SyncStep.Upload, e))
                        if e.Block and e.Scope == ServiceExceptionScope.Service: # Similarly, no behaviour to immediately abort the sync if an account-level exception is raised
                            excludedServices.append(destinationSvcRecord)
                        if not issubclass(e.__class__, ServiceWarning):
                            continue
                    except Exception as e:
                        tempSyncErrors[destinationSvcRecord._id].append({"Step": SyncStep.Upload, "Message": _formatExc()})
                        continue
                    logger.info("\t  Uploaded")
                    # flag as successful
                    db.connections.update({"_id": destinationSvcRecord._id},
                                          {"$addToSet": {"SynchronizedActivities": {"$each": activity.UIDs}}})

                    db.sync_stats.update({"ActivityID": activity.UID}, {"$addToSet": {"DestinationServices": destSvc.ID, "SourceServices": dlSvc.ID}, "$set": {"Distance": activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value, "Timestamp": datetime.utcnow()}}, upsert=True)
                del act
                del activity

                processedActivities += 1

            logger.info("Writing back service data")
            nonblockingSyncErrorsCount = 0
            blockingSyncErrorsCount = 0
            syncExclusionCount = 0
            for conn in serviceConnections:
                db.connections.update({"_id": conn._id}, {"$set": {"SyncErrors": tempSyncErrors[conn._id], "ExcludedActivities": tempSyncExclusions[conn._id]}})
                nonblockingSyncErrorsCount += len([x for x in tempSyncErrors[conn._id] if "Block" not in x or not x["Block"]])
                blockingSyncErrorsCount += len([x for x in tempSyncErrors[conn._id] if "Block" in x and x["Block"]])
                syncExclusionCount += len(tempSyncExclusions[conn._id].items())

            logger.info("Finalizing")
            # clear non-persisted extended auth details
            cachedb.extendedAuthDetails.remove({"ID": {"$in": connectedServiceIds}})
            # unlock the row
            update_values = {"$unset": {"SynchronizationWorker": None, "SynchronizationHost": None, "SynchronizationProgress": None, "SynchronizationStep": None}, "$set": {"NonblockingSyncErrorCount": nonblockingSyncErrorsCount, "BlockingSyncErrorCount": blockingSyncErrorsCount, "SyncExclusionCount": syncExclusionCount}}
            if null_next_sync_on_unlock:
                # Sometimes another worker would pick this record in the timespan between this update and the one in PerformGlobalSync that sets the true next sync time.
                # Hence, an option to unset the NextSynchronization in the same operation that releases the lock on the row.
                update_values["$unset"]["NextSynchronization"] = None
            db.users.update({"_id": user["_id"], "SynchronizationWorker": os.getpid(), "SynchronizationHost": socket.gethostname()}, update_values)
        except:
            # oops.
            logger.exception("Core sync exception")
        else:
            logger.info("Finished sync for " + str(user["_id"]))
        finally:
            _global_logger.removeHandler(logging_file_handler)
            logging_file_handler.flush()
            logging_file_handler.close()


class SynchronizationConcurrencyException(Exception):
    pass


class SyncStep:
    List = "list"
    Download = "download"
    Upload = "upload"

