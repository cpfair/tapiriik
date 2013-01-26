from tapiriik.services import Service
def providers(req):
	return {"service_providers":Service.List()}