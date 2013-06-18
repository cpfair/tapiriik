from tapiriik.database import db
from datetime import datetime, timedelta
#  total distance synced
distanceSynced = db.sync_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$Distance"}}}])["result"][0]["total"]

# sync time utilization
db.sync_worker_stats.remove({"Timestamp": {"$lt": datetime.utcnow() - timedelta(hours=1)}})  # clean up old records
timeUsed = db.sync_worker_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$TimeTaken"}}}])["result"][0]["total"]

# error/pending/locked stats
lockedSyncRecords = db.users.aggregate([
                                       {"$match": {"SynchronizationWorker": {"$ne": None}}},
                                       {"$group": {"_id": None, "count": {"$sum": 1}}}
                                       ])
if len(lockedSyncRecords["result"]) > 0:
    lockedSyncRecords = lockedSyncRecords["result"][0]["count"]
else:
    lockedSyncRecords = 0

pendingSynchronizations = db.users.aggregate([
                                             {"$match": {"NextSynchronization": {"$lt": datetime.utcnow()}}},
                                             {"$group": {"_id": None, "count": {"$sum": 1}}}
                                             ])
if len(pendingSynchronizations["result"]) > 0:
    pendingSynchronizations = pendingSynchronizations["result"][0]["count"]
else:
    pendingSynchronizations = 0

usersWithErrors = db.users.aggregate([
                                     {"$match": {"SyncErrorCount": {"$gt": 0}}},
                                     {"$group": {"_id": None, "count": {"$sum": 1}}}
                                     ])
if len(usersWithErrors["result"]) > 0:
    usersWithErrors = usersWithErrors["result"][0]["count"]
else:
    usersWithErrors = 0


totalErrors = db.users.aggregate([
   {"$group": {"_id": None,
               "total": {"$sum": "$SyncErrorCount"}}}
])

if len(totalErrors["result"]) > 0:
    totalErrors = totalErrors["result"][0]["sum"]
else:
    totalErrors = 0

db.sync_status_stats.insert({
        "Timestamp": datetime.utcnow(),
        "Locked": lockedSyncRecords,
        "Pending": pendingSynchronizations,
        "ErrorUsers": usersWithErrors,
        "TotalErrors": totalErrors
})

db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced, "TotalSyncTimeUsed": timeUsed, "Updated": datetime.utcnow()}}, upsert=True)
