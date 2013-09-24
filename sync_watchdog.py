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
    if alive and worker["Heartbeat"] < datetime.utcnow() - timedelta(minutes=45):  # I don't know what the longest running sync ever was...
        os.kill(worker["Process"], signal.SIGKILL)
        alive = False

    # Clear it from the database if it's not alive.
    if not alive:
        db.sync_workers.remove({"_id": worker["_id"]})


