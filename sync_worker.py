from datetime import datetime, timedelta
import os
print("Sync worker %s booting at %s" % (os.getpid(), datetime.now()))

from tapiriik.requests_lib import patch_requests_with_default_timeout, patch_requests_source_address
from tapiriik import settings
from tapiriik.database import db, close_connections
import time
import signal
import sys
import subprocess
import socket

RecycleInterval = 2 # Time spent rebooting workers < time spent wrangling Python memory management.

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_heartbeat(state, user=None):
    db.sync_workers.update({"Process": os.getpid(), "Host": socket.gethostname()}, {"$set": {"Heartbeat": datetime.utcnow(), "State": state, "User": user}})

print("Sync worker " + str(os.getpid()) + " initialized at " + str(datetime.now()))
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

Sync.InitializeWorkerBindings()

sync_heartbeat("ready")

Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat, version=WorkerVersion, max_users=RecycleInterval)

print("Sync worker shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid(), "Host": socket.gethostname()})
print("Closing database connections")
close_connections()
sys.stdout.flush()
