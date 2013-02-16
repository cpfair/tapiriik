from django import template
from tapiriik.services import Service
from tapiriik.database import db
register = template.Library()


@register.filter(name="svc_ids")
def IDs(value):
    return [x["Service"] for x in value]


@register.filter(name="svc_providers_except")
def exceptSvc(value):
    return [x for x in Service.List() if x not in providers(value)]


@register.filter(name="svc_conn_to_provider")
def provider(value):
    return Service.FromID(value["Service"])


@register.filter(name="svc_conns_to_providers")
def providers(value):
    return [provider(x) for x in value]


@register.filter(name="svc_populate_conns")
def fullRecords(conns):
    return db.connections.find({"_id": {"$in": [x["ID"] for x in conns]}})
