from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie


@ensure_csrf_cookie
def dashboard(req):
    return render(req, "dashboard.html")
