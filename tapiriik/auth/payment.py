from datetime import datetime
from tapiriik.database import db


class Payments:
	def LogPayment(id):
		# would use upsert, except that would reset the timestamp value
		existingRecord = db.payments.find_one({"Txn": id})
		if existingRecord is None:
			existingRecord = {"Txn": id, "Timestamp": datetime.utcnow()}
			db.payments.insert(existingRecord)
		return existingRecord

	def GetPayment(id):
		return db.payments.find_one({"Txn": id})

