from tapiriik.requests_lib import patch_requests_with_default_timeout, patch_requests_source_address
from tapiriik import settings
from tapiriik.database import db
import time
import datetime
import os
import signal
import sys
import subprocess
import socket

Run = True
RecycleInterval = 1 # Time spent rebooting workers < time spent wrangling Python memory management.

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_interrupt(signal, frame):
    global Run
    Run = False

signal.signal(signal.SIGINT, sync_interrupt)
signal.signal(signal.SIGUSR2, sync_interrupt)

def sync_heartbeat(state):
    db.sync_workers.update({"Process": os.getpid()}, {"$set": {"Heartbeat": datetime.datetime.utcnow(), "State": state}})

print("Sync worker starting at " + datetime.datetime.now().ctime() + " \n -> PID " + str(os.getpid()))
db.sync_workers.update({"Process": os.getpid()}, {"Process": os.getpid(), "Heartbeat": datetime.datetime.utcnow(), "Startup":  datetime.datetime.utcnow(),  "Version": WorkerVersion, "Host": socket.gethostname(), "State": "startup"}, upsert=True)
sys.stdout.flush()

patch_requests_with_default_timeout(timeout=60)

if isinstance(settings.HTTP_SOURCE_ADDR, list):
    settings.HTTP_SOURCE_ADDR = settings.HTTP_SOURCE_ADDR[settings.WORKER_INDEX % len(settings.HTTP_SOURCE_ADDR)]
    patch_requests_source_address((settings.HTTP_SOURCE_ADDR, 0))

print(" -> Index %s\n -> Interface %s" % (settings.WORKER_INDEX, settings.HTTP_SOURCE_ADDR))

# We defer including the main body of the application till here so the settings aren't captured before we've set them up.
# The better way would be to defer initializing services until they're requested, but it's 10:30 and this will work just as well.
from tapiriik.sync import Sync

while Run:
    cycleStart = datetime.datetime.utcnow()
    RecycleInterval -= Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat, version=WorkerVersion)
    if RecycleInterval <= 0:
    	break
    if (datetime.datetime.utcnow() - cycleStart).total_seconds() < 1:
        time.sleep(1)
    sync_heartbeat("idle")

print("Sync worker shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid()})
sys.stdout.flush()
