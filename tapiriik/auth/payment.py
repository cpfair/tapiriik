from datetime import datetime, timedelta
from tapiriik.database import db
from tapiriik.settings import PAYMENT_AMOUNT, PAYMENT_SYNC_DAYS
from bson.objectid import ObjectId

class Payments:
    def LogPayment(id, amount, initialAssociatedAccount, email):
        # pro-rate their expiry date
        expires_in_days = min(PAYMENT_SYNC_DAYS, float(amount) / float(PAYMENT_AMOUNT) * float(PAYMENT_SYNC_DAYS))
        # would use upsert, except that would reset the timestamp value
        existingRecord = db.payments.find_one({"Txn": id})
        if existingRecord is None:
            existingRecord = {
                "Txn": id,
                "Timestamp": datetime.utcnow(),
                "Expiry": datetime.utcnow() + timedelta(days=expires_in_days),
                "Amount": amount,
                "InitialAssociatedAccount": initialAssociatedAccount,
                "Email": email
            }
            db.payments.insert(existingRecord)
        return existingRecord

    def GetPayment(id=None, email=None):
        if id:
            return db.payments.find_one({"Txn": id})
        elif email:
            res = db.payments.find({"Email": email, "Expiry":{"$gt": datetime.utcnow()}}, limit=1)
            for payment in res:
                return payment

    def GenerateClaimCode(user, payment):
        db.payments_claim.remove({"Txn": payment["Txn"]})  # Remove any old codes, just to reduce the number kicking around at any one time.
        return str(db.payments_claim.insert({"Txn": payment["Txn"], "User": user["_id"], "Timestamp": datetime.utcnow()}))  # Return is the new _id, aka the claim code.

    def HasOutstandingClaimCode(user):
        return db.payments_claim.find_one({"User": user["_id"]}) is not None

    def ConsumeClaimCode(code):
        claim = db.payments_claim.find_one({"_id": ObjectId(code)})
        if not claim:
            return (None, None)
        db.payments_claim.remove(claim)
        return (db.users.find_one({"_id": claim["User"]}), db.payments.find_one({"Txn": claim["Txn"]}))

