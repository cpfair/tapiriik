from django import template
register = template.Library()


@register.filter(name="format_meters")
def meters_to_kms(value):
    if type(value) is not int:
        return 0
    return round(value / 1000)


@register.filter(name='dict_get')
def dict_get(tdict, key):
    if type(tdict) is not dict:
        tdict = tdict.__dict__
    return tdict.get(key, None)


@register.filter(name='format')
def format(format, var):
    return format.format(var)
