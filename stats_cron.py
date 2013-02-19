from tapiriik.database import db

#  total distance synced
distanceSynced = db.sync_stats.aggregate([{"$group": {"_id": None, "total": {"$sum": "$Distance"}}}])["result"][0]["total"]


db.stats.update({}, {"$set": {"TotalDistanceSynced": distanceSynced}}, upsert=True)
