from tapiriik.sync import Sync
from tapiriik.requests_lib import patch_requests_with_default_timeout
from tapiriik.database import db
import time
import datetime
import os
import signal
import sys
import subprocess
import socket

Run = True
RecycleInterval = 10 # Number of users processed before the worker is recycled. Meh.

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_interrupt(signal, frame):
    global Run
    Run = False

signal.signal(signal.SIGINT, sync_interrupt)

def sync_heartbeat(state):
    db.sync_workers.update({"Process": os.getpid()}, {"$set": {"Heartbeat": datetime.datetime.utcnow(), "State": state}})

print("Sync worker starting at " + datetime.datetime.now().ctime() + " pid " + str(os.getpid()))
db.sync_workers.update({"Process": os.getpid()}, {"Process": os.getpid(), "Heartbeat": datetime.datetime.utcnow(), "Startup":  datetime.datetime.utcnow(),  "Version": WorkerVersion, "Host": socket.gethostname(), "State": "startup"}, upsert=True)
sys.stdout.flush()

patch_requests_with_default_timeout(timeout=60)

while Run:
    cycleStart = datetime.datetime.utcnow()
    RecycleInterval -= Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat)
    if RecycleInterval <= 0:
    	break
    if (datetime.datetime.utcnow() - cycleStart).total_seconds() < 1:
        time.sleep(1)
    sync_heartbeat("idle")

print("Sync worker shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid()})
sys.stdout.flush()
