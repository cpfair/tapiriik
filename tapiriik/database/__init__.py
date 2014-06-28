from pymongo import MongoClient, MongoReplicaSetClient
from tapiriik.settings import MONGO_HOST, MONGO_REPLICA_SET, MONGO_CLIENT_OPTIONS, REDIS_HOST

# MongoDB

client_class = MongoClient if not MONGO_REPLICA_SET else MongoReplicaSetClient
_connection = client_class(host=MONGO_HOST, replicaSet=MONGO_REPLICA_SET, **MONGO_CLIENT_OPTIONS)
db = _connection["tapiriik"]
cachedb = _connection["tapiriik_cache"]
tzdb = _connection["tapiriik_tz"]

# Redis
if REDIS_HOST:
	import redis as redis_client
	redis = redis_client.Redis(host=REDIS_HOST)
else:
	redis = None # Must be defined

def close_connections():
	_connection.close()

import atexit
atexit.register(close_connections)