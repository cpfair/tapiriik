from pymongo import MongoClient
from tapiriik.settings import MONGO_HOST, REDIS_HOST
import redis as redis_client

# MongoDB
_connection = MongoClient(host=MONGO_HOST)
db = _connection["tapiriik"]
cachedb = _connection["tapiriik_cache"]
tzdb = _connection["tapiriik_tz"]

# Redis
redis = redis_client.StrictRedis(host=REDIS_HOST)
