from tapiriik.database import db
from tapiriik.messagequeue import mq
from tapiriik.sync import Sync
from datetime import datetime
from pymongo.read_preferences import ReadPreference
import kombu
import time

Sync.InitializeWorkerBindings()

producer = kombu.Producer(Sync._channel, Sync._exchange)

while True:
	queueing_at = datetime.utcnow()
	users = db.users.find(
				{
					"NextSynchronization": {"$lte": datetime.utcnow()}
				},
				{
					"_id": True,
					"SynchronizationHostRestriction": True
				},
				read_preference=ReadPreference.PRIMARY
			)
	scheduled_ids = set()
	for user in users:
		producer.publish({"user_id": str(user["_id"]), "queued_at": queueing_at}, routing_key=user["SynchronizationHostRestriction"] if "SynchronizationHostRestriction" in user and user["SynchronizationHostRestriction"] else "")
		scheduled_ids.add(user["_id"])
	print("Scheduled %d users at %s" % (len(scheduled_ids), datetime.utcnow()))
	db.users.update({"_id": {"$in": list(scheduled_ids)}}, {"$set": {"QueuedAt": queueing_at}, "$unset": {"NextSynchronization": True}}, multi=True)
	time.sleep(1)
