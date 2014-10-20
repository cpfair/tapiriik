from .provider_base import ExternalPaymentProvider
from tapiriik.database import db
from tapiriik.settings import MOTIVATO_PREMIUM_USERS_LIST_URL
import requests

class MotivatoExternalPaymentProvider(ExternalPaymentProvider):
	ID = "motivato"

	def RefreshPaymentStateForExternalIDs(self, external_ids):
		from tapiriik.services import Service, ServiceRecord
		external_ids = [str(x) for x in external_ids]
		connections = [ServiceRecord(x) for x in db.connections.find({"Service": "motivato", "ExternalID": {"$in": external_ids}})]
		users = db.users.find({"ConnectedServices.ID": {"$in": [x._id for x in connections]}})
		for user in users:
			my_connection = [x for x in connections if x._id in [y["ID"] for y in user["ConnectedServices"]]][0]
			# Defer to the actual service module, where all the session stuff is set up
			state = Service.FromID("motivato")._getPaymentState(my_connection)
			self.ApplyPaymentState(user, state, my_connection.ExternalID, duration=None)

	def RefreshPaymentState(self):
		from tapiriik.services import ServiceRecord
		from tapiriik.payments import Payments
		from tapiriik.auth import User

		external_ids = requests.get(MOTIVATO_PREMIUM_USERS_LIST_URL).json()
		connections = [ServiceRecord(x) for x in db.connections.find({"Service": "motivato", "ExternalID": {"$in": external_ids}})]
		users = list(db.users.find({"ConnectedServices.ID": {"$in": [x._id for x in connections]}}))
		payments = []

		# Pull relevant payment objects and associate with users
		for user in users:
			my_connection = [x for x in connections if x._id in [y["ID"] for y in user["ConnectedServices"]]][0]
			pmt = Payments.EnsureExternalPayment(self.ID, my_connection.ExternalID, duration=None)
			payments.append(pmt)
			User.AssociateExternalPayment(user, pmt, skip_deassoc=True)

		# Bulk-remove these payments from users who don't own them (more or less - it'll leave anyone who switched remote accounts)
		db.users.update({"_id": {"$nin": [x["_id"] for x in users]}}, {"$pull": {"ExternalPayments": {"_id": {"$in": [x["_id"] for x in payments]}}}}, multi=True)

		# We don't bother unsetting users who are no longer on the list - they'll be refreshed at their next sync


ExternalPaymentProvider.Register(MotivatoExternalPaymentProvider())