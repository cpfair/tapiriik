
class ExternalPaymentProvider:
    _providers = []
    def FromID(id):
        return [x for x in ExternalPaymentProvider._providers if x.ID == id][0]

    def Register(instance):
        ExternalPaymentProvider._providers.append(instance)

    ID = None
    def RefreshPaymentStateForExternalIDs(self, external_ids):
        raise NotImplemented

    def RefreshPaymentState(self):
        raise NotImplemented

    def ApplyPaymentState(self, user, state, externalID, duration=None):
        from tapiriik.payments import Payments
        from tapiriik.auth import User
        if state:
            pmt = Payments.EnsureExternalPayment(self.ID, externalID, duration)
            User.AssociateExternalPayment(user, pmt)
        else:
            Payments.ExpireExternalPayment(self.ID, externalID)
