from tapiriik.sync import Sync
from tapiriik.database import db
import time
import datetime
import os
import signal
import sys

Run = True

def sync_interrupt(signal, frame):
    global Run
    Run = False

signal.signal(signal.SIGINT, sync_interrupt)

print("Sync worker starting at " + datetime.datetime.now().ctime() + " pid " + str(os.getpid()))
sys.stdout.flush()

while Run:
    Sync.PerformGlobalSync()
    time.sleep(5)
    db.users.update({"$or": [{"SynchronizationWorker": os.getpid()},
                            {"LastSynchronization": {"$lt": datetime.datetime.utcnow() - datetime.timedelta(minutes=30)}}]},  # auto-release after 30 minutes
                            {"$unset": {"SynchronizationWorker": None}})
print("Sync worker shutting down cleanly")
sys.stdout.flush()