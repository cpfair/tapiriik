from tapiriik.database import db
from tapiriik.services import Service, APIException, APIAuthorizationException, ServiceException
from datetime import datetime, timedelta
import sys
import os
import traceback


class Sync:

    SyncInterval = timedelta(hours=1)
    MinimumSyncInterval = timedelta(minutes=10)

    def ScheduleImmediateSync(user, exhaustive=False):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow(), "NextSyncIsExhaustive": exhaustive}})

    def _determineRecipientServices(activity, allConnections):
        recipientServices = allConnections
        recipientServices = [conn for conn in recipientServices if activity.Type in Service.FromID(conn["Service"]).SupportedActivities
                                                                and ("SynchronizedActivities" not in conn or activity.UID not in conn["SynchronizedActivities"])
                                                                and conn not in [x["Connection"] for x in activity.UploadedTo]]
        return recipientServices

    def _accumulateActivities(svc, svcActivities, activityList):
        for act in svcActivities:
                existElsewhere = [x for x in activityList if x.UID == act.UID]
                if len(existElsewhere) > 0:
                    if act.TZ is not None and existElsewhere[0].TZ is None:
                        existElsewhere[0].TZ = act.TZ
                        existElsewhere[0].DefineTZ()
                    existElsewhere[0].UploadedTo += act.UploadedTo
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

        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]

        if len(connectedServiceIds) <= 1:
            return  # nothing's going anywhere anyways

        # mark this user as in-progress
        db.users.update({"_id": user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid()}})
        lockCheck = db.users.find_one({"_id": user["_id"], "SynchronizationWorker": os.getpid()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

        print ("Beginning sync for " + str(user["_id"]))

        serviceConnections = list(db.connections.find({"_id": {"$in": connectedServiceIds}}))
        activities = []

        for conn in serviceConnections:
            conn["SyncErrors"] = []
            svc = Service.FromID(conn["Service"])
            try:
                print ("\tRetrieving list from " + svc.ID)
                svcActivities = svc.DownloadActivityList(conn, exhaustive)
            except APIAuthorizationException as e:
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.NotAuthorized, "Message": e.Message, "Code": e.Code})
                continue
            except ServiceException as e:
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.Unknown, "Message": e.Message, "Code": e.Code})
                continue
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                continue
            Sync._accumulateActivities(svc, svcActivities, activities)

        origins = db.activity_origins.find({"ActivityUID": {"$in": [x.UID for x in activities]}})
            
        for activity in activities:
            if len(activity.UploadedTo) == 1:
                # we can log the origin of this activity
                db.activity_origins.update({"ActivityID": activity.UID}, {"ActivityUID": activity.UID, "Origin": {"Service": activity.UploadedTo[0]["Connection"]["Service"], "ExternalID": activity.UploadedTo[0]["Connection"]["Service"]}}, upsert=True)
                activity.Origin = activity.UploadedTo[0]["Connection"]
            else:
                knownOrigin = [x for x in origins if x["ActivityUID"] == activity.UID]
                if len(knownOrigin) > 0:
                    activity.Origin = [x for x in serviceConnections if knownOrigin[0]["Origin"]["Service"] == x["Service"] and knownOrigin[0]["Origin"]["ExternalID"] == x["ExternalID"]]
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
                try:
                    act = dlSvc.DownloadActivity(dlSvcRecord, activity)
                except APIAuthorizationException as e:
                    dlSvcRecord["SyncErrors"].append({"Step": SyncStep.Download, "Type": SyncError.NotAuthorized, "Message": e.Message, "Code": e.Code})
                    continue
                except ServiceException as e:
                    dlSvcRecord["SyncErrors"].append({"Step": SyncStep.Download, "Type": SyncError.Unknown, "Message": e.Message, "Code": e.Code})
                    continue
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    dlSvcRecord["SyncErrors"].append({"Step": SyncStep.Download, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                    continue
                else:
                    break  # succesfully got the activity, can stop now

            if act is None:  # couldn't download it from anywhere
                continue

            for destinationSvcRecord in recipientServices:
                
                flowException = False
                if hasattr(activity, "Origin"):
                    # we know the activity origin - do a more intuitive flow exception check
                    if User.CheckFlowException(user, activity.Origin, destinationSvcRecord):
                        flowException = True
                else:
                    for src in [x["Connection"] for x in activity.UploadedTo]
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
                    print("\t\tFlow exception for " + destSvc.ID)
                    continue;


                destSvc = Service.FromID(destinationSvcRecord["Service"])
                if destSvc.RequiresConfiguration and not Service.HasConfiguration(destinationSvcRecord):
                    continue  # not configured, so we won't even try
                try:
                    print("\t\tUploading to " + destSvc.ID)
                    destSvc.UploadActivity(destinationSvcRecord, act)
                except APIAuthorizationException as e:
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.Upload, "Type": SyncError.NotAuthorized, "Message": e.Message, "Code": e.Code})
                except ServiceException as e:
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.Upload, "Type": SyncError.Unknown, "Message": e.Message, "Code": e.Code})
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.Upload, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                    continue
                else:
                    # flag as successful
                    db.connections.update({"_id": destinationSvcRecord["_id"]},
                                          {"$addToSet": {"SynchronizedActivities": activity.UID}})

                    db.sync_stats.update({"ActivityID": activity.UID}, {"$inc": {"Destinations": 1}, "$set": {"Distance": activity.Distance, "Timestamp": datetime.utcnow()}}, upsert=True)

        for conn in serviceConnections:
            db.connections.update({"_id": conn["_id"]}, {"$set": {"SyncErrors": conn["SyncErrors"]}})

        # unlock the row
        db.users.update({"_id": user["_id"], "SynchronizationWorker": os.getpid()}, {"$unset": {"SynchronizationWorker": None}})
        print("Finished sync for " + str(user["_id"]))


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
