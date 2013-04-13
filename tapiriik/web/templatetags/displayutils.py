from django import template
register = template.Library()


@register.filter(name="format_meters")
def meters_to_kms(value):
    return round(value / 1000)


@register.filter(name='dict_get')
def dict_get(dict, key):
    return dict.get(key, None)


@register.filter(name='format')
def format(format, var):
    return format.format(var)
