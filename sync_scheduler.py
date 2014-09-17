from tapiriik.database import db
from tapiriik.messagequeue import mq
import kombu
from datetime import datetime
import time

channel = mq.channel()
exchange = kombu.Exchange("tapiriik-users", type="direct")(channel)
exchange.declare()
producer = kombu.Producer(channel, exchange)

scheduler_up_to_date = datetime.min

while True:
	queueing_at = datetime.utcnow()
	users = db.users.find(
				{
					"NextSynchronization": {"$lte": datetime.utcnow()},
					"NextSynchronization": {"$gte": scheduler_up_to_date},
					"$or": [
						{"QueuedAt": {"$lt": queueing_at}},
						{"QueuedAt": {"$exists": False}}
					]
				},
				{
					"_id": True,
					"SynchronizationHostRestriction": True
				}
			).sort("NextSynchronization")
	scheduled_ids = set()
	for user in users:
		producer.publish(str(user["_id"]), routing_key=user["SynchronizationHostRestriction"] if "SynchronizationHostRestriction" in user and user["SynchronizationHostRestriction"] else "")
		scheduled_ids.add(user["_id"])
	print("Scheduled %d users at %s" % (len(scheduled_ids), datetime.utcnow()))
	db.users.update({"_id": {"$in": list(scheduled_ids)}}, {"$set": {"SchedulerGeneration": scheduler_generation}}, multi=True)
	scheduler_generation += 1
	time.sleep(1)
