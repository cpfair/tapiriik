from tapiriik.database import db, close_connections
from datetime import datetime, timedelta
# I resisted calling this file sync_watchdog_watchdog.py, but that's what it is.
# Normally a watchdog process runs on each server and detects hung/crashed
# synchronization tasks, returning them to the queue for another worker to pick
# up. Except, if the entire server goes down, the watchdog no longer runs and
# users get stuck. So, we need a watchdog for the watchdogs. A separate process
# reschedules users left stranded by a failed server/process.

SERVER_WATCHDOG_TIMEOUT = timedelta(minutes=5)

print("Global sync watchdog run at %s" % datetime.now())

for host_record in db.sync_watchdogs.find():
    if datetime.utcnow() - host_record["Timestamp"] > SERVER_WATCHDOG_TIMEOUT:
        print("Releasing users held by %s (last check-in %s)" % (host_record["Host"], host_record["Timestamp"]))
        db.users.update({"SynchronizationHost": host_record["Host"]}, {"$unset": {"SynchronizationWorker": True}}, multi=True)
        db.sync_workers.remove({"Host": host_record["Host"]}, multi=True)
        db.sync_watchdogs.remove({"_id": host_record["_id"]})

close_connections()
