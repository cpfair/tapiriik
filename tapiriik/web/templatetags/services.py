from django import template
from tapiriik.services import Service
register = template.Library()


@register.filter(name="svc_ids")
def IDs(value):
    return [x["Service"] for x in value]


@register.filter(name="svc_except")
def exceptSvc(value):
    return [x for x in Service.List() if x not in value]


@register.filter(name="svc_providers")
def providers(value):
    return [Service.FromID(x["Service"]) for x in value]
