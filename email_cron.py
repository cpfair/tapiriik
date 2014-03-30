from tapiriik.database import db
from tapiriik.web.email import generate_message_from_template, send_email
from tapiriik.services import Service
from tapiriik.settings import WITHDRAWN_SERVICES
from datetime import datetime, timedelta
import os
import math
os.environ["DJANGO_SETTINGS_MODULE"] = "tapiriik.settings"

# Renewal emails
now = datetime.utcnow()
expiry_window_open = now - timedelta(days=28)
expiry_window_close = now
expired_payments = db.payments.find({"Expiry": {"$gt": expiry_window_open, "$lt": expiry_window_close}, "ReminderEmailSent": {"$ne": True}})

for expired_payment in expired_payments:
	connected_user = db.users.find_one({"Payments._id": expired_payment["_id"]})
	print("Composing renewal email for %s" % expired_payment["Email"])
	if not connected_user:
		print("...no associated user")
		continue
	connected_services_names = [Service.FromID(x["Service"]).DisplayName for x in connected_user["ConnectedServices"] if x["Service"] not in WITHDRAWN_SERVICES]

	if len(connected_services_names) == 0:
		connected_services_names = ["fitness tracking"]
	elif len(connected_services_names) == 1:
		connected_services_names.append("other fitness tracking")
	if len(connected_services_names) > 1:
		connected_services_names = ", ".join(connected_services_names[:-1]) + " and " + connected_services_names[-1] + " accounts"
	else:
		connected_services_names = connected_services_names[0] + " accounts"

	subscription_days = round((expired_payment["Expiry"] - expired_payment["Timestamp"]).total_seconds() / (60 * 60 * 24))
	subscription_fuzzy_time_map = {
		(0, 8): "few days",
		(8, 31): "few weeks",
		(31, 150): "few months",
		(150, 300): "half a year",
		(300, 999): "year"
	}
	subscription_fuzzy_time = [v for k,v in subscription_fuzzy_time_map.items() if k[0] <= subscription_days and k[1] > subscription_days][0]

	activity_records = db.activity_records.find_one({"UserID": connected_user["_id"]})
	total_distance_synced = None
	if activity_records:
		total_distance_synced = sum([x["Distance"] for x in activity_records["Activities"] if x["Distance"]])
		total_distance_synced = math.floor(total_distance_synced/1000 / 100) * 100

	context = {
		"account_list": connected_services_names,
		"subscription_days": subscription_days,
		"subscription_fuzzy_time": subscription_fuzzy_time,
		"distance": total_distance_synced
	}
	message, plaintext_message = generate_message_from_template("email/payment_renew.html", context)
	send_email(expired_payment["Email"], "tapiriik automatic synchronization expiry", message, plaintext_message=plaintext_message)
	db.payments.update({"_id": expired_payment["_id"]}, {"$set": {"ReminderEmailSent": True}})
	print("...sent")
