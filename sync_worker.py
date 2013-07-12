from tapiriik.sync import Sync
from tapiriik.database import db
import time
import datetime
import os
import signal
import sys
import subprocess

Run = True

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_interrupt(signal, frame):
    global Run
    Run = False

signal.signal(signal.SIGINT, sync_interrupt)

def sync_heartbeat():
	db.sync_workers.update({"Process": os.getpid()}, {"$set": {"Heartbeat": datetime.datetime.utcnow()}})

print("Sync worker starting at " + datetime.datetime.now().ctime() + " pid " + str(os.getpid()))
db.sync_workers.update({"Process": os.getpid()}, {"Process": os.getpid(), "Heartbeat": datetime.datetime.utcnow(), "Version": WorkerVersion}, upsert=True)
sys.stdout.flush()

while Run:
    Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat)

    time.sleep(5)
    sync_heartbeat()

print("Sync worker shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid()})
sys.stdout.flush()
