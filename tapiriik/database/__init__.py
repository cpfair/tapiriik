from pymongo import MongoClient
_connection = MongoClient()
db = _connection["tapiriik"]
db.table.insert({"hey":"u"})