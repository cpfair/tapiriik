from pymongo import MongoClient, MongoReplicaSetClient
from tapiriik.settings import MONGO_HOST, MONGO_REPLICA_SET, MONGO_CLIENT_OPTIONS, REDIS_HOST

# MongoDB

client_class = MongoClient if not MONGO_REPLICA_SET else MongoReplicaSetClient
if MONGO_REPLICA_SET:
	MONGO_CLIENT_OPTIONS["replicaSet"] = MONGO_REPLICA_SET

_connection = client_class(host=MONGO_HOST, **MONGO_CLIENT_OPTIONS)

db = _connection["tapiriik"]
cachedb = _connection["tapiriik_cache"]
tzdb = _connection["tapiriik_tz"]
# The main db currently has an unfortunate lock contention rate
ratelimit = _connection["tapiriik_ratelimit"]

# Redis
if REDIS_HOST:
	import redis as redis_client
	redis = redis_client.Redis(host=REDIS_HOST)
else:
	redis = None # Must be defined

def close_connections():
	try:
		_connection.close()
	except:
		pass

import atexit
atexit.register(close_connections)
