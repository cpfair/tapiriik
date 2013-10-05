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
                                     {"$match": {"NonblockingSyncErrorCount": {"$gt": 0}}},
                                     {"$group": {"_id": None, "count": {"$sum": 1}}}
                                     ])
if len(usersWithErrors["result"]) > 0:
    usersWithErrors = usersWithErrors["result"][0]["count"]
else:
    usersWithErrors = 0


totalErrors = db.users.aggregate([
   {"$group": {"_id": None,
               "total": {"$sum": "$NonblockingSyncErrorCount"}}}
])

if len(totalErrors["result"]) > 0:
    totalErrors = totalErrors["result"][0]["total"]
else:
    totalErrors = 0

db.sync_status_stats.insert({
        "Timestamp": datetime.utcnow(),
        "Locked": lockedSyncRecords,
        "Pending": pendingSynchronizations,
        "ErrorUsers": usersWithErrors,
        "TotalErrors": totalErrors,
        "SyncTimeUsed": timeUsed
})

db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced, "TotalSyncTimeUsed": timeUsed, "Updated": datetime.utcnow()}}, upsert=True)


def aggregateCommonErrors():
    from bson.code import Code
    # The exception message always appears right before "LOCALS:"
    map_operation = Code(
        "function(){"
            "var errorMatch = new RegExp(/\\n([^\\n]+)\\n\\nLOCALS:/);"
            "if (!this.SyncErrors) return;"
            "var id = this._id;"
            "this.SyncErrors.forEach(function(error){"
                "var message = error.Message.match(errorMatch)[1];"
                "emit(message.substring(0, 60),{count:1, connections:[id], exemplar:message});"
            "});"
        "}"
        )
    reduce_operation = Code(
        "function(key, item){"
            "var reduced = {count:0, connections:[]};"
            "var connection_collections = [];"
            "item.forEach(function(error){"
                "reduced.count+=error.count;"
                "reduced.exemplar = error.exemplar;"
                "connection_collections.push(error.connections);"
            "});"
            "reduced.connections = reduced.connections.concat.apply(reduced.connections, connection_collections);"
            "return reduced;"
        "}")
    db.connections.map_reduce(map_operation, reduce_operation, "common_sync_errors") #, finalize=finalize_operation
    # We don't need to do anything with the result right now, just leave it there to appear in the dashboard

aggregateCommonErrors()
