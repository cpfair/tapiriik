from tapiriik.database import db, cachedb
from tapiriik.services import Service, ServiceRecord, APIAuthorizationException, APIExcludeActivity, ServiceException, ServiceWarning
from tapiriik.settings import USER_SYNC_LOGS, DISABLED_SERVICES
from datetime import datetime, timedelta
import sys
import os
import traceback
import pprint
import copy
import pytz
import random
import logging
import logging.handlers
import pymongo

# Set this up seperate from the logger used in this scope, so services logging messages are caught and logged into user's files.
_global_logger = logging.getLogger("tapiriik")

_global_logger.setLevel(logging.DEBUG)
logging_console_handler = logging.StreamHandler(sys.stdout)
logging_console_handler.setLevel(logging.DEBUG)
logging_console_handler.setFormatter(logging.Formatter('%(message)s'))
_global_logger.addHandler(logging_console_handler)

logger = logging.getLogger("tapiriik.sync.worker")

def _formatExc():
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
        recipientServices = allConnections
        recipientServices = [conn for conn in recipientServices if activity.Type in conn.Service.SupportedActivities
                                                                and (not hasattr(conn, "SynchronizedActivities") or not len([x for x in activity.UIDs if x in conn.SynchronizedActivities]))
                                                                and conn not in [x["Connection"] for x in activity.UploadedTo]]
        return recipientServices

    def _fromSameService(activityA, activityB):
        otherSvcs = [y["Connection"]._id for y in activityB.UploadedTo]
        for uploadA in activityA.UploadedTo:
            if uploadA["Connection"]._id in otherSvcs:
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

    def _accumulateActivities(svc, svcActivities, activityList):
        # Yep, abs() works on timedeltas
        activityStartLeeway = timedelta(minutes=3)
        timezoneErrorPeriod = timedelta(hours=14)
        from tapiriik.services.interchange import ActivityType
        for act in svcActivities:
            act.UIDs = [act.UID]
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
                              # Sometimes wacky stuff happens and we get two activities with the same mm:ss but different hh, because of a TZ issue somewhere along the line
                              # So, we check for any activities +/- 14 hours that have the same minutes and seconds values
                              #  (14 hours because Kiribati)
                              # There's a 1/3600 chance that two activities in the same 14 hour period would intersect and be merged together
                              # But, given the fact that most users have maybe 0.05 activities per this period, it's an acceptable tradeoff
                              (x.StartTime is not None and
                               act.StartTime is not None and
                               abs(act.StartTime.replace(tzinfo=None)-x.StartTime.replace(tzinfo=None)) < timezoneErrorPeriod and
                               act.StartTime.replace(tzinfo=None).time().replace(hour=0) == x.StartTime.replace(tzinfo=None).time().replace(hour=0)
                               )
                              ]
            if len(existElsewhere) > 0:
                # we don't merge the exclude values here, since at this stage the services have the option of just not returning those activities
                if act.TZ is not None and existElsewhere[0].TZ is None:
                    existElsewhere[0].TZ = act.TZ
                    existElsewhere[0].DefineTZ()
                # tortuous merging logic is tortuous
                existElsewhere[0].StartTime = Sync._coalesceDatetime(existElsewhere[0].StartTime, act.StartTime)
                existElsewhere[0].EndTime = Sync._coalesceDatetime(existElsewhere[0].EndTime, act.EndTime, knownTz=existElsewhere[0].StartTime.tzinfo)
                existElsewhere[0].Name = existElsewhere[0].Name if existElsewhere[0].Name is not None else act.Name
                existElsewhere[0].Waypoints = existElsewhere[0].Waypoints if len(existElsewhere[0].Waypoints) > 0 else act.Waypoints
                existElsewhere[0].Type = ActivityType.PickMostSpecific([existElsewhere[0].Type, act.Type])

                existElsewhere[0].UploadedTo += act.UploadedTo
                existElsewhere[0].UIDs += act.UIDs  # I think this is merited
                act.UIDs = existElsewhere[0].UIDs  # stop the circular inclusion, not that it matters
                continue
            activityList.append(act)

    def _determineEligibleRecipientServices(activity, recipientServices, excludedServices, user):
        from tapiriik.auth import User
        eligibleServices = []
        for destinationSvcRecord in recipientServices:
            if destinationSvcRecord in excludedServices:
                logger.info("\t\tExcluded " + destinationSvcRecord.Service.ID)
                continue  # we don't know for sure if it needs to be uploaded, hold off for now
            flowException = False
            if hasattr(activity, "Origin"):
                # we know the activity origin - do a more intuitive flow exception check
                if User.CheckFlowException(user, activity.Origin, destinationSvcRecord):
                    flowException = True
            else:
                for src in [x["Connection"] for x in activity.UploadedTo]:
                    if User.CheckFlowException(user, src, destinationSvcRecord):
                        flowException = True
                        break
                #  this isn't an absolute failure - it's possible we could still take an indirect route
                #  at this point there's no knowledge of the origin of this activity, so this behaviour would happen anyways at the next sync
                if flowException:
                    for secondLevelSrc in [x for x in recipientServices if x != destinationSvcRecord]:
                        if not User.CheckFlowException(user, secondLevelSrc, destinationSvcRecord):
                            flowException = False
                            break
            if flowException:
                logger.info("\t\tFlow exception for " + destinationSvcRecord.Service.ID)
                continue
            destSvc = destinationSvcRecord.Service
            if destSvc.RequiresConfiguration(destinationSvcRecord):
                logger.info("\t\t" + destSvc.ID + " not configured")
                continue  # not configured, so we won't even try
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

    def PerformGlobalSync(heartbeat_callback=None):
        from tapiriik.auth import User
        users = db.users.find({"NextSynchronization": {"$lte": datetime.utcnow()}, "SynchronizationWorker": None})  # mongoDB doesn't let you query by size of array to filter 1- and 0-length conn lists :\
        for user in users:
            syncStart = datetime.utcnow()

            # Always to an exhaustive sync if there were errors
            #   Sometimes services report that uploads failed even when they succeeded.
            #   So we need to verify the full state of the accounts.
            exhaustive = "NextSyncIsExhaustive" in user and user["NextSyncIsExhaustive"] is True
            if "SyncErrorCount" in user and user["SyncErrorCount"] > 0:
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
                db.sync_worker_stats.insert({"Timestamp": datetime.utcnow(), "Worker": os.getpid(), "TimeTaken": syncTime})
            if heartbeat_callback:
                heartbeat_callback()

    def PerformUserSync(user, exhaustive=False, null_next_sync_on_unlock=False, heartbeat_callback=None):
        # And thus begins the monolithic sync function that's a pain to test.
        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]

        if len(connectedServiceIds) <= 1:
            return  # nothing's going anywhere anyways

        # mark this user as in-progress
        db.users.update({"_id": user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid(), "SynchronizationProgress": 0}})
        lockCheck = db.users.find_one({"_id": user["_id"], "SynchronizationWorker": os.getpid()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

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
                if heartbeat_callback:
                    heartbeat_callback()
                svc = conn.Service
                tempSyncErrors[conn._id] = []
                conn.SyncErrors = []

                # Remove temporary exclusions (live tracking etc).
                tempSyncExclusions[conn._id] = dict((k, v) for k, v in (conn.ExcludedActivities if conn.ExcludedActivities else {}).items() if v["Permanent"])
                if conn.ExcludedActivities:
                    del conn.ExcludedActivities  # Otherwise the exception messages get really, really, really huge and break mongodb.

                if svc.ID in DISABLED_SERVICES:
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

                try:
                    logger.info("\tRetrieving list from " + svc.ID)
                    svcActivities, svcExclusions = svc.DownloadActivityList(conn, exhaustive)
                except (APIAuthorizationException, ServiceException, ServiceWarning) as e:
                    etype = SyncError.NotAuthorized if issubclass(e.__class__, APIAuthorizationException) else SyncError.System
                    tempSyncErrors[conn._id].append({"Step": SyncStep.List, "Type": etype, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                    excludedServices.append(conn)
                    if not issubclass(e.__class__, ServiceWarning):
                        continue
                except Exception as e:
                    tempSyncErrors[conn._id].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": _formatExc()})
                    excludedServices.append(conn)
                    continue
                Sync._accumulateExclusions(conn, svcExclusions, tempSyncExclusions)
                Sync._accumulateActivities(svc, svcActivities, activities)

            origins = list(db.activity_origins.find({"ActivityUID": {"$in": [x.UID for x in activities]}}))
            activitiesWithOrigins = [x["ActivityUID"] for x in origins]
            for activity in activities:
                updated_database = False
                if len(activity.UploadedTo) == 1:
                    if not len(excludedServices):  # otherwise it could be incorrectly recorded
                        # we can log the origin of this activity
                        if activity.UID not in activitiesWithOrigins:  # No need to hammer the database updating these when they haven't changed
                            logger.info("\t\t Updating db with origin for proceeding activity")
                            db.activity_origins.insert({"ActivityUID": activity.UID, "Origin": {"Service": activity.UploadedTo[0]["Connection"].Service.ID, "ExternalID": activity.UploadedTo[0]["Connection"].ExternalID}})
                        activity.Origin = activity.UploadedTo[0]["Connection"]
                else:
                    if activity.UID in activitiesWithOrigins:
                        knownOrigin = [x for x in origins if x["ActivityUID"] == activity.UID]
                        connectedOrigins = [x for x in serviceConnections if knownOrigin[0]["Origin"]["Service"] == x.Service.ID and knownOrigin[0]["Origin"]["ExternalID"] == x.ExternalID]
                        if len(connectedOrigins) > 0:  # they might have disconnected it
                            activity.Origin = connectedOrigins[0]
                        else:
                            activity.Origin = ServiceRecord(knownOrigin[0]["Origin"])  # I have it on good authority that this will work
                logger.info("\t" + str(activity) + " " + str(activity.UID[:3]) + " from " + str([x["Connection"].Service.ID for x in activity.UploadedTo]))

            totalActivities = len(activities)
            processedActivities = 0
            for activity in activities:
                if heartbeat_callback:
                    heartbeat_callback()
                # we won't need this now, but maybe later
                db.connections.update({"_id": {"$in": [x["Connection"]._id for x in activity.UploadedTo]}},
                                      {"$addToSet": {"SynchronizedActivities": activity.UID}},
                                      multi=True)

                recipientServices = Sync._determineRecipientServices(activity, serviceConnections)
                if len(recipientServices) == 0:
                    totalActivities -= 1  # doesn't count
                    continue

                # this is after the above exit point since it's the most frequent case - want to avoid DB churn
                if totalActivities <= 0:
                    syncProgress = 1
                else:
                    syncProgress = max(0, min(1, processedActivities / totalActivities))

                db.users.update({"_id": user["_id"]}, {"$set": {"SynchronizationProgress": syncProgress}})

                # download the full activity record
                logger.info("\tActivity " + str(activity.UID) + " to " + str([x.Service.ID for x in recipientServices]))

                eligibleServices = Sync._determineEligibleRecipientServices(activity=activity, recipientServices=recipientServices, excludedServices=excludedServices, user=user)

                if not len(eligibleServices):
                    logger.info("\t No eligible destinations")
                    totalActivities -= 1  # Again, doesn't really count.
                    continue
                act = None
                for dlSvcUploadRec in activity.UploadedTo:
                    dlSvcRecord = dlSvcUploadRec["Connection"]  # I guess in the future we could smartly choose which for >1, or at least roll over on error
                    dlSvc = dlSvcRecord.Service
                    logger.info("\t from " + dlSvc.ID)
                    if activity.UID in tempSyncExclusions[dlSvcRecord._id]:
                        logger.info("\t\t has activity exclusion logged")
                        continue
                    workingCopy = copy.copy(activity)  # we can hope
                    try:
                        workingCopy = dlSvc.DownloadActivity(dlSvcRecord, workingCopy)
                    except (APIAuthorizationException, ServiceException, ServiceWarning) as e:
                        etype = SyncError.NotAuthorized if issubclass(e.__class__, APIAuthorizationException) else SyncError.System
                        tempSyncErrors[conn._id].append({"Step": SyncStep.Download, "Type": etype, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                        if not issubclass(e.__class__, ServiceWarning):
                            continue
                    except APIExcludeActivity as e:
                        e.Activity = workingCopy
                        Sync._accumulateExclusions(dlSvcRecord, e, tempSyncExclusions)
                        continue
                    except Exception as e:
                        tempSyncErrors[dlSvcRecord._id].append({"Step": SyncStep.Download, "Type": SyncError.System, "Message": _formatExc()})
                        continue
                    try:
                        workingCopy.CheckSanity()
                    except:
                        Sync._accumulateExclusions(dlSvcRecord, APIExcludeActivity("Sanity check failed " + _formatExc(), activity=workingCopy), tempSyncExclusions)
                        continue
                    else:
                        act = workingCopy
                        break  # succesfully got the activity + passed sanity checks, can stop now

                if act is None:  # couldn't download it from anywhere, or the places that had it said it was broken
                    processedActivities += 1  # we tried
                    continue

                for destinationSvcRecord in eligibleServices:
                    destSvc = destinationSvcRecord.Service
                    try:
                        logger.info("\t\tUploading to " + destSvc.ID)
                        destSvc.UploadActivity(destinationSvcRecord, act)
                    except (APIAuthorizationException, ServiceException, ServiceWarning) as e:
                        etype = SyncError.NotAuthorized if issubclass(e.__class__, APIAuthorizationException) else SyncError.System
                        tempSyncErrors[destinationSvcRecord._id].append({"Step": SyncStep.Upload, "Type": etype, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                        if not issubclass(e.__class__, ServiceWarning):
                            continue
                    except Exception as e:
                        tempSyncErrors[destinationSvcRecord._id].append({"Step": SyncStep.Upload, "Type": SyncError.System, "Message": _formatExc()})
                        continue
                    # flag as successful
                    db.connections.update({"_id": destinationSvcRecord._id},
                                          {"$addToSet": {"SynchronizedActivities": activity.UID}})

                    db.sync_stats.update({"ActivityID": activity.UID}, {"$addToSet": {"DestinationServices": destSvc.ID, "SourceServices": dlSvc.ID}, "$set": {"Distance": activity.Distance, "Timestamp": datetime.utcnow()}}, upsert=True)
                act.Waypoints = activity.Waypoints = []  # Free some memory

                processedActivities += 1

            allSyncErrors = []
            syncExclusionCount = 0
            for conn in serviceConnections:
                db.connections.update({"_id": conn._id}, {"$set": {"SyncErrors": tempSyncErrors[conn._id], "ExcludedActivities": tempSyncExclusions[conn._id]}})
                allSyncErrors += tempSyncErrors[conn._id]
                syncExclusionCount += len(tempSyncExclusions[conn._id].items())

            # clear non-persisted extended auth details
            cachedb.extendedAuthDetails.remove({"ID": {"$in": connectedServiceIds}})
            # unlock the row
            update_values = {"$unset": {"SynchronizationWorker": None, "SynchronizationProgress": None}, "$set": {"SyncErrorCount": len(allSyncErrors), "SyncExclusionCount": syncExclusionCount}}
            if null_next_sync_on_unlock:
                # Sometimes another worker would pick this record in the timespan between this update and the one in PerformGlobalSync that sets the true next sync time.
                # Hence, an option to unset the NextSynchronization in the same operation that releases the lock on the row.
                update_values["$unset"]["NextSynchronization"] = None
            db.users.update({"_id": user["_id"], "SynchronizationWorker": os.getpid()}, update_values)
        except:
            # oops.
            logger.exception("Core sync exception")
        else:
            logger.info("Finished sync for " + str(user["_id"]))
        finally:
            _global_logger.removeHandler(logging_file_handler)
            logging_file_handler.close()

class SynchronizationConcurrencyException(Exception):
    pass


class SyncStep:
    List = "list"
    Download = "download"
    Upload = "upload"


class SyncError:
    System = "system"
    Unknown = "unkown"
    NotAuthorized = "authorization"
