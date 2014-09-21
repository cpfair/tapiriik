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

    def ReversePayment(id):
        # Mark the transaction, and pull it from any users who have it.
        db.payments.update({"Txn": id}, {"$set": {"Reversed": True}})
        db.users.update({"Payments.Txn": id}, {"$pull": {"Payments": {"Txn": id}}}, multi=True)

    def GetPayment(id=None, email=None):
        if id:
            return db.payments.find_one({"Txn": id, "Reversed": {"$ne": True}})
        elif email:
            res = db.payments.find({"Email": email, "Expiry":{"$gt": datetime.utcnow()}, "Reversed": {"$ne": True}}, limit=1)
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

    def EnsureExternalPayment(provider, externalID, duration=None):
        existingRecord = db.external_payments.find_one({
            "Provider": provider,
            "ExternalID": externalID,
            "$or": [
                    {"Expiry": {"$exists": False}},
                    {"Expiry": None},
                    {"Expiry": {"$gte": datetime.utcnow()}}
                ]
            })
        if existingRecord is None:
            existingRecord = {
                "Provider": provider,
                "ExternalID": externalID,
                "Timestamp": datetime.utcnow(),
                "Expiry": datetime.utcnow() + duration if duration else None
            }
            db.external_payments.insert(existingRecord)
        return existingRecord

    def ExpireExternalPayment(provider, externalID):
        now = datetime.utcnow()
        db.external_payments.update(
            {
                "Provider": provider,
                "ExternalID": externalID,
                "$or": [
                    {"Expiry": {"$exists": False}},
                    {"Expiry": None},
                ]
            }, {
                "$set": {"Expiry": now}
            })

        # Wrangle the user copies - man, should have used an RDBMS
        expired_payment = db.external_payments.find_one({"Provider": provider, "ExternalID": externalID, "Expiry": now})
        # Could be already expired, no need to rerun the update
        if expired_payment:
            affected_user_ids = [x["_id"] for x in db.users.find({"ExternalPayments._id": expired_payment["_id"]}, {"_id": True})]
            db.users.update({"_id": {"$in": affected_user_ids}}, {"$pull": {"ExternalPayments": {"_id": expired_payment["_id"]}}}, multi=True)
            db.users.update({"_id": {"$in": affected_user_ids}}, {"$addToSet": {"ExternalPayments": expired_payment}}, multi=True)

    def GetAndActivatePromo(code):
        promo = db.promo_codes.find_one({"Code": code})
        if not promo:
            return None

        if "FirstClaimedTimestamp" not in promo:
            promo["FirstClaimedTimestamp"] = datetime.utcnow()

        # In seconds!
        if "Duration" in promo:
            promo["Expiry"] = promo["FirstClaimedTimestamp"] + timedelta(seconds=promo["Duration"])
        else:
            promo["Expiry"] = None

        # Write back, as we may have just activated it
        db.promo_codes.save(promo)

        return promo
