from pymongo import MongoClient
_connection = MongoClient()
db = _connection["tapiriik"]
cachedb = _connection["tapiriik_cache"]
