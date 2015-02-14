from tapiriik.database import db
from tapiriik.auth import User
from tapiriik.services import Service
import datetime
import logging
import json
from bson.objectid import ObjectId
logger = logging.getLogger(__name__)

class RollbackTask:
    def __new__(cls, dbRec):
        if not dbRec:
            return None
        return super(RollbackTask, cls).__new__(cls)

    def __init__(self, dbRec):
        self.__dict__.update(dbRec)

    def _create(user):
        # Pull all the records that need to be rolled back
        logger.info("Finding activities for %s" % user["_id"])
        conns = User.GetConnectionRecordsByUser(user)
        my_services = [conn.Service.ID for conn in conns]
        my_ext_ids = [conn.ExternalID for conn in conns]
        logger.info("Scanning uploads table for %s accounts with %s extids" % (my_services, my_ext_ids))
        uploads = db.uploaded_activities.find({"Service": {"$in": my_services}, "UserExternalID": {"$in": my_ext_ids}})
        pending_deletions = {}
        for upload in uploads:
            svc = upload["Service"]
            upload_id = upload["ExternalID"]
            svc_ext_id = upload["UserExternalID"]
            # Filter back down to the pairing we actually need
            if my_services.index(svc) != my_ext_ids.index(svc_ext_id):
                continue
            if svc not in pending_deletions:
                pending_deletions[svc] = []
            pending_deletions[svc].append(upload_id)

        # Another case of "I should have an ORM"
        return RollbackTask({"PendingDeletions": pending_deletions})

    def Create(user):
        task = RollbackTask._create(user)
        uid = db.rollback_tasks.insert({"PendingDeletions": task.PendingDeletions, "Created": datetime.datetime.utcnow(), "UserID": user["_id"]})
        logger.info("Created rollback task %s" % uid)
        task._id = uid
        return task

    def Get(id):
        dbRec = db.rollback_tasks.find_one({"_id": ObjectId(id)})
        if not dbRec:
            return
        return RollbackTask(dbRec)

    def json(self):
        # Augment with the requisite URLs
        self.ActivityURLs = {svc: {} for svc in self.PendingDeletions.keys()}
        for svc_id, urls in self.ActivityURLs.items():
            svc = Service.FromID(svc_id)
            for upload in self.PendingDeletions[svc_id]:
                try:
                    urls[upload] = svc.UserUploadedActivityURL(upload)
                except NotImplementedError:
                    pass
        self.PendingDeletionCount = sum([len(v) for k, v in self.PendingDeletions.items()])
        dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime)  or isinstance(obj, datetime.date) else str(obj)
        return json.dumps(self.__dict__, default=dthandler)

    def Schedule(self):
        db.rollback_tasks.update({"_id": self._id}, {"$set": {"Scheduled": datetime.datetime.utcnow()}})
        from rollback_worker import schedule_rollback_task
        schedule_rollback_task(str(self._id))

    def Execute(self):
        logger.info("Starting rollback %s" % self._id)
        deletion_status = {}
        user = User.Get(self.UserID)
        for svc_id, upload_ids in self.PendingDeletions.items():
            svcrec = User.GetConnectionRecord(user, svc_id)
            deletion_status[svc_id] = {}
            if not svcrec.Service.SupportsActivityDeletion:
                continue
            for upload_id in upload_ids:
                logger.info("Deleting activity %s on %s" % (upload_id, svc_id))
                try:
                    svcrec.Service.DeleteActivity(svcrec, upload_id)
                except Exception as e:
                    deletion_status[svc_id][str(upload_id)] = False
                    logger.exception("Deletion failed - %s" % e)
                else:
                    deletion_status[svc_id][str(upload_id)] = True
                db.rollback_tasks.update({"_id": self._id}, {"$set": {"DeletionStatus": deletion_status}})
        logger.info("Finished rollback %s" % self._id)
