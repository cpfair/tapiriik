from tapiriik.database import ratelimit as rl_db
from pymongo.read_preferences import ReadPreference
from datetime import datetime, timedelta
import math

class RateLimitExceededException(Exception):
	pass

class RateLimit:
	def Limit(key):
		current_limits = rl_db.limits.find({"Key": key}, {"Max": 1, "Count": 1})
		for limit in current_limits:
			if limit["Max"] < limit["Count"]:
				# We can't continue without exceeding this limit
				# Don't want to halt the synchronization worker to wait for 15min-1 hour
				# So...
				raise RateLimitExceededException()
		rl_db.limits.update({"Key": key}, {"$inc": {"Count": 1}}, multi=True)

	def Refresh(key, limits):
		# Limits is in format [(timespan, max-count),...]
		# The windows are anchored at midnight
		# The timespan is used to uniquely identify limit instances between runs
		midnight = datetime.combine(datetime.utcnow().date(), datetime.min.time())
		time_since_midnight = (datetime.utcnow() - midnight)

		rl_db.limits.remove({"Key": key, "Expires": {"$lt": datetime.utcnow()}})
		current_limits = list(rl_db.limits.with_options(read_preference=ReadPreference.PRIMARY).find({"Key": key}, {"Duration": 1}))
		missing_limits = [x for x in limits if x[0].total_seconds() not in [limit["Duration"] for limit in current_limits]]
		for limit in missing_limits:
			window_start = midnight + timedelta(seconds=math.floor(time_since_midnight.total_seconds()/limit[0].total_seconds()) * limit[0].total_seconds())
			window_end = window_start + limit[0]
			rl_db.limits.insert({"Key": key, "Count": 0, "Duration": limit[0].total_seconds(), "Max": limit[1], "Expires": window_end})
