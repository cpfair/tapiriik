from pymongo import MongoClient
from tapiriik.settings import MONGO_HOST
_connection = MongoClient(host=MONGO_HOST)
db = _connection["tapiriik"]
cachedb = _connection["tapiriik_cache"]
