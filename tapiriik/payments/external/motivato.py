from .provider_base import ExternalPaymentProvider
from tapiriik.database import db

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

ExternalPaymentProvider.Register(MotivatoExternalPaymentProvider())