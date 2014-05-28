from tapiriik.requests_lib import patch_requests_with_default_timeout, patch_requests_source_address
from tapiriik import settings
from tapiriik.database import db
import time
from datetime import datetime, timedelta
import os
import signal
import sys
import subprocess
import socket

Run = True
RecycleInterval = 1 # Time spent rebooting workers < time spent wrangling Python memory management.
MinCycleTime = timedelta(seconds=30) # No need to hammer the database given the number of sync workers I have

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_interrupt(signal, frame):
    global Run
    Run = False

signal.signal(signal.SIGINT, sync_interrupt)
signal.signal(signal.SIGUSR2, sync_interrupt)

def sync_heartbeat(state):
    db.sync_workers.update({"Process": os.getpid(), "Host": socket.gethostname()}, {"$set": {"Heartbeat": datetime.utcnow(), "State": state}})

print("Sync worker starting at " + datetime.now().ctime() + " \n -> PID " + str(os.getpid()))
db.sync_workers.update({"Process": os.getpid(), "Host": socket.gethostname()}, {"Process": os.getpid(), "Heartbeat": datetime.utcnow(), "Startup":  datetime.utcnow(),  "Version": WorkerVersion, "Host": socket.gethostname(), "Index": settings.WORKER_INDEX, "State": "startup"}, upsert=True)
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
    cycleStart = datetime.utcnow() # Avoid having synchronization fall down during DST setback
    RecycleInterval -= Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat, version=WorkerVersion)
    # Put this before the recycle shutdown, otherwise it'll quit and get rebooted ASAP
    remaining_cycle_time = MinCycleTime - (datetime.utcnow() - cycleStart)
    if remaining_cycle_time > timedelta(0):
        sync_heartbeat("idle-spin")
        time.sleep(remaining_cycle_time.total_seconds())
    if RecycleInterval <= 0:
    	break
    sync_heartbeat("idle")

print("Sync worker shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid(), "Host": socket.gethostname()})
sys.stdout.flush()
