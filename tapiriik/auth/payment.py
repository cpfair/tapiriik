from datetime import datetime, timedelta
from tapiriik.database import db
from tapiriik.settings import PAYMENT_AMOUNT, PAYMENT_SYNC_DAYS


class Payments:
    def LogPayment(id, amount=None):
        # pro-rate their expiry date
        expires_in_days = min(PAYMENT_SYNC_DAYS, amount / PAYMENT_AMOUNT * PAYMENT_SYNC_DAYS)
        # would use upsert, except that would reset the timestamp value
        existingRecord = db.payments.find_one({"Txn": id})
        if existingRecord is None:
            existingRecord = {"Txn": id, "Timestamp": datetime.utcnow(), "Expiry": datetime.utcnow() + timedelta(days=expires_in_days)}
            db.payments.insert(existingRecord)
        return existingRecord

    def GetPayment(id):
        return db.payments.find_one({"Txn": id})

