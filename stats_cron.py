from tapiriik.database import db, close_connections
from datetime import datetime, timedelta

# total distance synced
distanceSyncedAggr = list(db.sync_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$Distance"}}}]))
if distanceSyncedAggr:
    distanceSynced = distanceSyncedAggr[0]["total"]
else:
    distanceSynced = 0

# last 24hr, for rate calculation
lastDayDistanceSyncedAggr = list(db.sync_stats.aggregate([{"$match": {"Timestamp": {"$gt": datetime.utcnow() - timedelta(hours=24)}}}, {"$group": {"_id": None, "total": {"$sum": "$Distance"}}}]))
if lastDayDistanceSyncedAggr:
    lastDayDistanceSynced = lastDayDistanceSyncedAggr[0]["total"]
else:
    lastDayDistanceSynced = 0

# similarly, last 1hr
lastHourDistanceSyncedAggr = list(db.sync_stats.aggregate([{"$match": {"Timestamp": {"$gt": datetime.utcnow() - timedelta(hours=1)}}}, {"$group": {"_id": None, "total": {"$sum": "$Distance"}}}]))
if lastHourDistanceSyncedAggr:
    lastHourDistanceSynced = lastHourDistanceSyncedAggr[0]["total"]
else:
    lastHourDistanceSynced = 0
# sync wait time, to save making 1 query/sec-user-browser
queueHead = list(db.users.find({"QueuedAt": {"$lte": datetime.utcnow()}, "SynchronizationWorker": None, "SynchronizationHostRestriction": {"$exists": False}}, {"QueuedAt": 1}).sort("QueuedAt").limit(10))
queueHeadTime = timedelta(0)
if len(queueHead):
    for queuedUser in queueHead:
        queueHeadTime += datetime.utcnow() - queuedUser["QueuedAt"]
    queueHeadTime /= len(queueHead)

# sync time utilization
db.sync_worker_stats.remove({"Timestamp": {"$lt": datetime.utcnow() - timedelta(hours=1)}})  # clean up old records
timeUsedAgg = list(db.sync_worker_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$TimeTaken"}}}]))
totalSyncOps = db.sync_worker_stats.count()
if timeUsedAgg:
    timeUsed = timeUsedAgg[0]["total"]
    avgSyncTime = timeUsed / totalSyncOps
else:
    timeUsed = 0
    avgSyncTime = 0

# error/pending/locked stats
lockedSyncRecords = list(db.users.aggregate([
                                       {"$match": {"SynchronizationWorker": {"$ne": None}}},
                                       {"$group": {"_id": None, "count": {"$sum": 1}}}
                                       ]))
if len(lockedSyncRecords) > 0:
    lockedSyncRecords = lockedSyncRecords[0]["count"]
else:
    lockedSyncRecords = 0

pendingSynchronizations = list(db.users.aggregate([
                                             {"$match": {"NextSynchronization": {"$lt": datetime.utcnow()}}},
                                             {"$group": {"_id": None, "count": {"$sum": 1}}}
                                             ]))
if len(pendingSynchronizations) > 0:
    pendingSynchronizations = pendingSynchronizations[0]["count"]
else:
    pendingSynchronizations = 0

usersWithErrors = list(db.users.aggregate([
                                     {"$match": {"NonblockingSyncErrorCount": {"$gt": 0}}},
                                     {"$group": {"_id": None, "count": {"$sum": 1}}}
                                     ]))
if len(usersWithErrors) > 0:
    usersWithErrors = usersWithErrors[0]["count"]
else:
    usersWithErrors = 0


totalErrors = list(db.users.aggregate([
   {"$group": {"_id": None,
               "total": {"$sum": "$NonblockingSyncErrorCount"}}}
]))

if len(totalErrors) > 0:
    totalErrors = totalErrors[0]["total"]
else:
    totalErrors = 0

db.sync_status_stats.insert({
        "Timestamp": datetime.utcnow(),
        "Locked": lockedSyncRecords,
        "Pending": pendingSynchronizations,
        "ErrorUsers": usersWithErrors,
        "TotalErrors": totalErrors,
        "SyncTimeUsed": timeUsed,
        "SyncQueueHeadTime": queueHeadTime.total_seconds()
})

db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced, "LastDayDistanceSynced": lastDayDistanceSynced, "LastHourDistanceSynced": lastHourDistanceSynced, "TotalSyncTimeUsed": timeUsed, "AverageSyncDuration": avgSyncTime, "LastHourSynchronizationCount": totalSyncOps, "QueueHeadTime": queueHeadTime.total_seconds(), "Updated": datetime.utcnow()}}, upsert=True)


def aggregateCommonErrors():
    from bson.code import Code
    # The exception message always appears right before "LOCALS:"
    map_operation = Code(
        "function(){"
            "if (!this.SyncErrors) return;"
            "var errorMatch = new RegExp(/\\n([^\\n]+)\\n\\nLOCALS:/);"
            "var id = this._id;"
            "var svc = this.Service;"
            "var now = new Date();"
            "this.SyncErrors.forEach(function(error){"
                "var message = error.Message.match(errorMatch)[1];"
                "var key = {service: svc, stem: message.substring(0, 60)};"
                "var recency_score = error.Timestamp ? (now - error.Timestamp)/1000 : 0;"
                "emit(key, {count:1, ts_count: error.Timestamp ? 1 : 0, recency: recency_score, connections:[id], exemplar:message});"
            "});"
        "}"
        )
    reduce_operation = Code(
        "function(key, item){"
            "var reduced = {count:0, ts_count:0, connections:[], recency: 0};"
            "var connection_collections = [];"
            "item.forEach(function(error){"
                "reduced.count+=error.count;"
                "reduced.ts_count+=error.ts_count;"
                "reduced.recency+=error.recency;"
                "reduced.exemplar = error.exemplar;"
                "connection_collections.push(error.connections);"
            "});"
            "reduced.connections = reduced.connections.concat.apply(reduced.connections, connection_collections);"
            "return reduced;"
        "}")
    finalize_operation = Code(
        "function(key, res){"
            "res.recency_avg = res.recency / res.ts_count;"
            "return res;"
        "}"
    )
    db.connections.map_reduce(map_operation, reduce_operation, "common_sync_errors", finalize=finalize_operation) 
    # We don't need to do anything with the result right now, just leave it there to appear in the dashboard

aggregateCommonErrors()

close_connections()

