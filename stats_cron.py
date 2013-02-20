from tapiriik.database import db
from datetime import datetime
#  total distance synced
distanceSynced = db.sync_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$Distance"}}}])["result"][0]["total"]


db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced, "Updated": datetime.utcnow()}}, upsert=True)
