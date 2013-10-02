from tapiriik.database import db
import os
import signal
import socket
from datetime import timedelta, datetime

for worker in db.sync_workers.find({"Host": socket.gethostname()}):
    # Does the process still exist?
    alive = True
    try:
        os.kill(worker["Process"], 0)
    except os.error:
        alive = False

    # Has it been stalled for too long?
    if worker["State"] == "sync-list":
        timeout = timedelta(minutes=45)  # This can take a loooooooong time
    else:
        timeout = timedelta(minutes=10)  # But everything else shouldn't

    if alive and worker["Heartbeat"] < datetime.utcnow() - timeout:
        os.kill(worker["Process"], signal.SIGKILL)
        alive = False

    # Clear it from the database if it's not alive.
    if not alive:
        db.sync_workers.remove({"_id": worker["_id"]})


