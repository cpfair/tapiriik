from tapiriik.sync import Sync
from tapiriik.database import db
import time
import datetime
import os

print("Sync worker starting at " + datetime.datetime.now().ctime() + " pid " + str(os.getpid()))
while True:
    Sync.PerformGlobalSync()
    time.sleep(5)
    db.users.update({"$or": [{"SynchronizationWorker": os.getpid()},
                            {"LastSynchronization": {"$lt": datetime.datetime.utcnow() - datetime.timedelta(minutes=30)}}]},  # auto-release after 30 minutes
                            {"$unset": {"SynchronizationWorker": None}})
