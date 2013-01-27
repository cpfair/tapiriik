from tapiriik.database import db
from tapiriik.services import Service
from datetime import datetime


class Sync:
    def ScheduleImmediateSync(user):
        db.users.update({"_id": user["_id"]}, {"$set": {"NextSynchronization": datetime.utcnow()}})

    def PerformUserSync(user):
        connectedServiceIds = user["ConnectedServices"]
        serviceConnections = list(db.connections.find({"_id": {"$in": connectedServiceIds}}))
        activities = []

        for conn in serviceConnections:
            svc = Service.FromID(conn["Service"])
            svcActivities = svc.DownloadActivityList(conn)

            for act in svcActivities:
                existElsewhere = [x for x in activities if x.UID == act.UID]
                if len(existElsewhere) > 0:
                    existElsewhere[0].UploadedTo += act.UploadedTo
                    continue
                activities.append(act)

        for activity in activities:
            # we won't need this now, but maybe later
            db.connections.update({"_id": {"$in": [x["_id"] for x in activity.UploadedTo]}},\
                {"$addToSet": {"SynchronizedActivities": activity.UID}},\
                multi=True)
            # python really needs LINQ
            recipientServices = serviceConnections
            recipientServices = [conn for conn in recipientServices if conn not in activity.UploadedTo]
            recipientServices = [conn for conn in recipientServices if "SynchronizedActivities" not in conn or activity.UID not in conn["SynchronizedActivities"]]
            # UPLOAD AWAY!
