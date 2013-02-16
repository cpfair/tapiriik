from tapiriik.database import db
from tapiriik.services import Service, APIException, APIAuthorizationException
from datetime import datetime, timedelta
import sys
import os
import traceback


class Sync:

    SyncInterval = timedelta(hours=1)
    MinimumSyncInterval = timedelta(minutes=10)

    def ScheduleImmediateSync(user):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})

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
                    existElsewhere[0].UploadedTo += act.UploadedTo
                    continue
                activityList.append(act)

    def PerformGlobalSync():
        users = db.users.find({"NextSynchronization": {"$lte": datetime.utcnow()}, "SynchronizationWorker": None})  # mongoDB doesn't let you query by size of array to filter 1- and 0-length conn lists :\
        for user in users:
            try:
                Sync.PerformUserSync(user)
            except SynchronizationConcurrencyException:
                pass  # another worker picked them
            else:
                db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow() + Sync.SyncInterval, "LastSynchronization": datetime.utcnow()}})

    def PerformUserSync(user, exhaustive=False):
        # mark this user as in-progress
        db.users.update({"_id": user["_id"], "SynchronizationWorker": None}, {"$set": {"SynchronizationWorker": os.getpid()}})
        lockCheck = db.users.find_one({"_id": user["_id"], "SynchronizationWorker": os.getpid()})
        if lockCheck is None:
            raise SynchronizationConcurrencyException  # failed to get lock

        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]

        if len(connectedServiceIds) <= 1:
            return  # nothing's going anywhere anyways

        serviceConnections = list(db.connections.find({"_id": {"$in": connectedServiceIds}}))
        activities = []

        for conn in serviceConnections:
            conn["SyncErrors"] = []
            svc = Service.FromID(conn["Service"])
            try:
                svcActivities = svc.DownloadActivityList(conn, exhaustive)
            except APIAuthorizationException as e:
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.NotAuthorized, "Message": e.Message})
                continue
            except APIException as e:
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.Unknown, "Message": e.Message})
                continue
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                conn["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                continue
            Sync._accumulateActivities(svc, svcActivities, activities)

        for activity in activities:
            print (str(activity) + " from " + activity.UploadedTo[0]["Connection"]["Service"] + " ct " + str(len(activity.UploadedTo)))

        for activity in activities:
            # we won't need this now, but maybe later
            db.connections.update({"_id": {"$in": [x["Connection"]["_id"] for x in activity.UploadedTo]}},\
                {"$addToSet": {"SynchronizedActivities": activity.UID}},\
                multi=True)

            recipientServices = Sync._determineRecipientServices(activity, serviceConnections)
            if len(recipientServices) == 0:
                continue
            # download the full activity record
            print("Activity " + str(activity.UID) + " to " + str([x["Service"] for x in recipientServices]))
            dlSvcRecord = activity.UploadedTo[0]["Connection"]  # I guess in the future we could smartly choose which for >1, or at least roll over on error
            dlSvc = Service.FromID(dlSvcRecord["Service"])
            try:
                act = dlSvc.DownloadActivity(dlSvcRecord, activity)
            except APIAuthorizationException as e:
                dlSvcRecord["SyncErrors"].append({"Step": SyncStep.Download, "Type": SyncError.NotAuthorized, "Message": e.Message})
                continue
            except APIException as e:
                dlSvcRecord["SyncErrors"].append({"Step": SyncStep.Download, "Type": SyncError.Unknown, "Message": e.Message})
                continue
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                dlSvcRecord["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                continue

            for destinationSvcRecord in recipientServices:
                destSvc = Service.FromID(destinationSvcRecord["Service"])
                try:
                    destSvc.UploadActivity(destinationSvcRecord, act)
                except APIAuthorizationException as e:
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.Upload, "Type": SyncError.NotAuthorized, "Message": e.Message})
                except APIException as e:
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.Upload, "Type": SyncError.Unknown, "Message": e.Message})
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    destinationSvcRecord["SyncErrors"].append({"Step": SyncStep.List, "Type": SyncError.System, "Message": '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback))})
                    continue
                else:
                    # flag as successful
                    db.connections.update({"_id": destinationSvcRecord["_id"]},\
                    {"$addToSet": {"SynchronizedActivities": activity.UID}})

        for conn in serviceConnections:
            db.connections.update({"_id": conn["_id"]}, {"$set": {"SyncErrors": conn["SyncErrors"]}})

        # unlock the row
        db.users.update({"_id": user["_id"], "SynchronizationWorker": os.getpid()}, {"$unset": {"SynchronizationWorker": None}})


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
