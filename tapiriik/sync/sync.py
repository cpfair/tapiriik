from tapiriik.database import db
from tapiriik.services import Service, APIException, APIAuthorizationException, ServiceException
from datetime import datetime, timedelta
import sys
import os
import traceback
import pprint

def _formatExc():
    print("Dumping exception")
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = exc_traceback
    while tb.tb_next:
        tb = tb.tb_next
    frame = tb.tb_frame
    return '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + "\nLOCALS:\n" + '\n'.join([str(k) + "=" + pprint.pformat(v) for k, v in frame.f_locals.items()])
    del tb


class Sync:

    SyncInterval = timedelta(hours=1)
    MinimumSyncInterval = timedelta(minutes=2)

    def ScheduleImmediateSync(user, exhaustive=None):
        if exhaustive is None:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})
        else:
            db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow(), "NextSyncIsExhaustive": exhaustive}})

    def SetNextSyncIsExhaustive(user, exhaustive=False):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSyncIsExhaustive": exhaustive}})

    def _determineRecipientServices(activity, allConnections):
        recipientServices = allConnections
        recipientServices = [conn for conn in recipientServices if activity.Type in Service.FromID(conn["Service"]).SupportedActivities
                                                                and ("SynchronizedActivities" not in conn or not [x for x in activity.UIDs if x in conn["SynchronizedActivities"]])
                                                                and conn not in [x["Connection"] for x in activity.UploadedTo]]
        return recipientServices

    def _fromSameService(activityA, activityB):
        otherSvcs = [y["Connection"]["_id"] for y in activityB.UploadedTo]
        for uploadA in activityA.UploadedTo:
            if uploadA["Connection"]["_id"] in otherSvcs:
                return True
        return False

    def _accumulateActivities(svc, svcActivities, activityList):
        for act in svcActivities:
            act.UIDs = [act.UID]
            if len(act.Waypoints) > 0:
                act.EnsureTZ()
            existElsewhere = [x for x in activityList if x.UID == act.UID or
                              ( not Sync._fromSameService(x, act) and
                                x.StartTime is not None and
                               act.StartTime is not None and
                               (act.StartTime.tzinfo is not None) == (x.StartTime.tzinfo is not None) and
                               abs((act.StartTime-x.StartTime).total_seconds()) < 60 * 3
                               )
                              ]
            if len(existElsewhere) > 0:
                if act.TZ is not None and existElsewhere[0].TZ is None:
                    existElsewhere[0].TZ = act.TZ
                    existElsewhere[0].DefineTZ()
                existElsewhere[0].UploadedTo += act.UploadedTo
                existElsewhere[0].UIDs += act.UIDs  # I think this is merited
                act.UIDs = existElsewhere[0].UIDs  # stop the circular inclusion, not that it matters
                continue
            activityList.append(act)

    def PerformGlobalSync():
        from tapiriik.auth import User
        users = db.users.find({"NextSynchronization": {"$lte": datetime.utcnow()}, "SynchronizationWorker": None})  # mongoDB doesn't let you query by size of array to filter 1- and 0-length conn lists :\
        for user in users:
            syncStart = datetime.utcnow()
            try:
                Sync.PerformUserSync(user, "NextSyncIsExhaustive" in user and user["NextSyncIsExhaustive"] is True)
            except SynchronizationConcurrencyException:
                pass  # another worker picked them
            else:
                nextSync = None
                if User.HasActivePayment(user):
                    nextSync = datetime.utcnow() + Sync.SyncInterval
                db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": nextSync, "LastSynchronization": datetime.utcnow()}, "$unset": {"NextSyncIsExhaustive": None}})
                syncTime = (datetime.utcnow() - syncStart).total_seconds()
                db.sync_worker_stats.insert({"Timestamp": datetime.utcnow(), "Worker": os.getpid(), "TimeTaken": syncTime})

    def PerformUserSync(user, exhaustive=False):
        from tapiriik.auth import User
        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]

        if len(connectedServiceIds) <= 1:
            return  # nothing's going anywhere anyways

        # mark this user as in-progress
        db.users.update({"_id": user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid()}})
        lockCheck = db.users.find_one({"_id": user["_id"], "SynchronizationWorker": os.getpid()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

        print ("Beginning sync for " + str(user["_id"]) + " at " + datetime.now().ctime())

        serviceConnections = list(db.connections.find({"_id": {"$in": connectedServiceIds}}))
        activities = []

        excludedServices = []

        tempSyncErrors = {}

        for conn in serviceConnections:
            tempSyncErrors[conn["_id"]] = []
            conn["SyncErrors"] = []
            svc = Service.FromID(conn["Service"])
            try:
                print ("\tRetrieving list from " + svc.ID)
                svcActivities = svc.DownloadActivityList(conn, exhaustive)
            except APIAuthorizationException as e:
                tempSyncErrors[conn["_id"]].append({"Step": SyncStep.List, "Type": SyncError.NotAuthorized, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                excludedServices.append(conn["_id"])
                continue
            except ServiceException as e:
                tempSyncErrors[conn["_id"]].append({"Step": SyncStep.List, "Type": SyncError.Unknown, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                excludedServices.append(conn["_id"])
                continue
            except Exception as e:
                tempSyncErrors[conn["_id"]].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": _formatExc()})
                excludedServices.append(conn["_id"])
                continue
            Sync._accumulateActivities(svc, svcActivities, activities)

        origins = db.activity_origins.find({"ActivityUID": {"$in": [x.UID for x in activities]}})

        for activity in activities:
            if len(activity.UploadedTo) == 1:
                # we can log the origin of this activity
                db.activity_origins.update({"ActivityID": activity.UID}, {"ActivityUID": activity.UID, "Origin": {"Service": activity.UploadedTo[0]["Connection"]["Service"], "ExternalID": activity.UploadedTo[0]["Connection"]["ExternalID"]}}, upsert=True)
                activity.Origin = activity.UploadedTo[0]["Connection"]
            else:
                knownOrigin = [x for x in origins if x["ActivityUID"] == activity.UID]
                if len(knownOrigin) > 0:
                    activity.Origin = [x for x in serviceConnections if knownOrigin[0]["Origin"]["Service"] == x["Service"] and knownOrigin[0]["Origin"]["ExternalID"] == x["ExternalID"]][0]
            print ("\t" + str(activity) + " " + str(activity.UID[:3]) + " from " + str([x["Connection"]["Service"] for x in activity.UploadedTo]))

        for activity in activities:
            # we won't need this now, but maybe later
            db.connections.update({"_id": {"$in": [x["Connection"]["_id"] for x in activity.UploadedTo]}},
                                  {"$addToSet": {"SynchronizedActivities": activity.UID}},
                                  multi=True)

            recipientServices = Sync._determineRecipientServices(activity, serviceConnections)
            if len(recipientServices) == 0:
                continue
            # download the full activity record
            print("\tActivity " + str(activity.UID) + " to " + str([x["Service"] for x in recipientServices]))
            act = None
            for dlSvcUploadRec in activity.UploadedTo:

                dlSvcRecord = dlSvcUploadRec["Connection"]  # I guess in the future we could smartly choose which for >1, or at least roll over on error
                dlSvc = Service.FromID(dlSvcRecord["Service"])
                print("\t from " + dlSvc.ID)
                try:
                    act = dlSvc.DownloadActivity(dlSvcRecord, activity)
                except APIAuthorizationException as e:
                    tempSyncErrors[dlSvcRecord["_id"]].append({"Step": SyncStep.Download, "Type": SyncError.NotAuthorized, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                    continue
                except ServiceException as e:
                    tempSyncErrors[dlSvcRecord["_id"]].append({"Step": SyncStep.Download, "Type": SyncError.Unknown, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                    continue
                except Exception as e:
                    tempSyncErrors[dlSvcRecord["_id"]].append({"Step": SyncStep.Download, "Type": SyncError.System, "Message": _formatExc()})
                    continue
                else:
                    try:
                        act.CheckSanity()
                    except:
                        tempSyncErrors[dlSvcRecord["_id"]].append({"Step": SyncStep.Download, "Type": SyncError.System, "Message": _formatExc()})
                        act = None
                        continue
                    else:
                        break  # succesfully got the activity + passed sanity checks, can stop now

            if act is None:  # couldn't download it from anywhere
                continue

            for destinationSvcRecord in recipientServices:
                if destinationSvcRecord["_id"] in excludedServices:
                    print("\t\tExcluded " + destinationSvcRecord["Service"])
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
                    print("\t\tFlow exception for " + destinationSvcRecord["Service"])
                    continue

                destSvc = Service.FromID(destinationSvcRecord["Service"])
                if destSvc.RequiresConfiguration and not Service.HasConfiguration(destinationSvcRecord):
                    continue  # not configured, so we won't even try
                try:
                    print("\t\tUploading to " + destSvc.ID)
                    destSvc.UploadActivity(destinationSvcRecord, act)
                except APIAuthorizationException as e:
                    tempSyncErrors[destinationSvcRecord["_id"]].append({"Step": SyncStep.Upload, "Type": SyncError.NotAuthorized, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                except ServiceException as e:
                    tempSyncErrors[destinationSvcRecord["_id"]].append({"Step": SyncStep.Upload, "Type": SyncError.Unknown, "Message": e.Message + "\n" + _formatExc(), "Code": e.Code})
                except Exception as e:
                    tempSyncErrors[destinationSvcRecord["_id"]].append({"Step": SyncStep.Upload, "Type": SyncError.System, "Message": _formatExc()})
                else:
                    # flag as successful
                    db.connections.update({"_id": destinationSvcRecord["_id"]},
                                          {"$addToSet": {"SynchronizedActivities": activity.UID}})

                    db.sync_stats.update({"ActivityID": activity.UID}, {"$inc": {"Destinations": 1}, "$set": {"Distance": activity.Distance, "Timestamp": datetime.utcnow()}}, upsert=True)

        allSyncErrors = []
        for conn in serviceConnections:
            db.connections.update({"_id": conn["_id"]}, {"$set": {"SyncErrors": tempSyncErrors[conn["_id"]]}})
            allSyncErrors += tempSyncErrors[conn["_id"]]

        # unlock the row
        db.users.update({"_id": user["_id"], "SynchronizationWorker": os.getpid()}, {"$unset": {"SynchronizationWorker": None}, "$set": {"SyncErrorCount": len(allSyncErrors)}})
        print("Finished sync for " + str(user["_id"]) + " at " + datetime.now().ctime())


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
