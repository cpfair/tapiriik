from tapiriik.database import db
from datetime import datetime, timedelta
#  total distance synced
distanceSynced = db.sync_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$Distance"}}}])["result"][0]["total"]

#sync time utilization
db.sync_worker_stats.remove({"Timestamp": {"$lt": datetime.utcnow() - timedelta(hours=1)}})  # clean up old records
timeUsed = db.sync_worker_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$TimeTaken"}}}])["result"][0]["total"]

db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced, "TotalSyncTimeUsed": timeUsed, "Updated": datetime.utcnow()}}, upsert=True)
