from django import template
from tapiriik.auth import User
register = template.Library()


@register.filter(name="has_active_payment")
def HasActivePayment(user):
    return User.HasActivePayment(user)
