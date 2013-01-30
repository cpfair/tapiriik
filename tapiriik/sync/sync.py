from tapiriik.database import db
from tapiriik.services import Service
from datetime import datetime


class Sync:
    def ScheduleImmediateSync(user):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})

    def _determineRecipientServices(activity, allConnections):
        recipientServices = allConnections
        recipientServices = [conn for conn in recipientServices if activity.Type in Service.FromID(conn["Service"]).SupportedActivities]
        recipientServices = [conn for conn in recipientServices if "SynchronizedActivities" not in conn or activity.UID not in conn["SynchronizedActivities"]]
        return recipientServices

    def _accumulateActivitiesToSync(svc, svcActivities, activityList):
        for act in svcActivities:
                existElsewhere = [x for x in activityList if x.UID == act.UID]
                if len(existElsewhere) > 0:
                    existElsewhere[0].UploadedTo += act.UploadedTo
                    continue
                activityList.append(act)

    def PerformUserSync(user):
        connectedServiceIds = [x["ID"] for x in user["ConnectedServices"]]
        serviceConnections = list(db.connections.find({"_id": {"$in": connectedServiceIds}}))
        activities = []

        for conn in serviceConnections:
            svc = Service.FromID(conn["Service"])
            svcActivities = svc.DownloadActivityList(conn)
            Sync._accumulateActivitiesToSync(svc, svcActivities, activities)

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
            dlSvcRecord = activity.UploadedTo[0]["Connection"]  # I guess in the future we could smartly choose which for >1
            dlSvc = Service.FromID(dlSvcRecord["Service"])
            dlSvc.DownloadActivity(dlSvcRecord, activity)
