from tapiriik.database import db
from datetime import datetime


class Sync:
    def ScheduleImmediateSync(user):
        db.users.update({"_id":user["_id"]},{"$set":{"NextSynchronization":datetime.utcnow()}})
