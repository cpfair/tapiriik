from datetime import datetime, timedelta
import os
# I'm trying to track down where some missing seconds are going in the sync process
# Will grep these out of the log at some later date
def worker_message(state):
    print("Sync worker %d %s at %s" % (os.getpid(), state, datetime.now()))

worker_message("booting")

from tapiriik.requests_lib import patch_requests_with_default_timeout, patch_requests_source_address
from tapiriik import settings
from tapiriik.database import db, close_connections
import sys
import subprocess
import socket

RecycleInterval = 2 # Time spent rebooting workers < time spent wrangling Python memory management.

oldCwd = os.getcwd()
WorkerVersion = subprocess.Popen(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, cwd=os.path.dirname(__file__)).communicate()[0].strip()
os.chdir(oldCwd)

def sync_heartbeat(state, user=None):
    db.sync_workers.update({"Process": os.getpid(), "Host": socket.gethostname()}, {"$set": {"Heartbeat": datetime.utcnow(), "State": state, "User": user}})

worker_message("initialized")
db.sync_workers.update({"Process": os.getpid(), "Host": socket.gethostname()}, {"Process": os.getpid(), "Heartbeat": datetime.utcnow(), "Startup":  datetime.utcnow(),  "Version": WorkerVersion, "Host": socket.gethostname(), "Index": settings.WORKER_INDEX, "State": "startup"}, upsert=True)
sys.stdout.flush()

patch_requests_with_default_timeout(timeout=60)

if isinstance(settings.HTTP_SOURCE_ADDR, list):
    settings.HTTP_SOURCE_ADDR = settings.HTTP_SOURCE_ADDR[settings.WORKER_INDEX % len(settings.HTTP_SOURCE_ADDR)]
    patch_requests_source_address((settings.HTTP_SOURCE_ADDR, 0))

print(" %d -> Index %s\n -> Interface %s" % (os.getpid(), settings.WORKER_INDEX, settings.HTTP_SOURCE_ADDR))

# We defer including the main body of the application till here so the settings aren't captured before we've set them up.
# The better way would be to defer initializing services until they're requested, but it's 10:30 and this will work just as well.
from tapiriik.sync import Sync

Sync.InitializeWorkerBindings()

sync_heartbeat("ready")

worker_message("ready")

Sync.PerformGlobalSync(heartbeat_callback=sync_heartbeat, version=WorkerVersion, max_users=RecycleInterval)

worker_message("shutting down cleanly")
db.sync_workers.remove({"Process": os.getpid(), "Host": socket.gethostname()})
close_connections()
worker_message("shut down")
sys.stdout.flush()
