from django.core.exceptions import MiddlewareNotUsed


class ServiceWebStartup:
    def __init__(self):
        from tapiriik.services import Service
        Service.WebInit()
        raise MiddlewareNotUsed
