from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from tapiriik.services import Service


@ensure_csrf_cookie
def dashboard(req):
    user = req.user
    

    return render(req,"dashboard.html",{"user": user})
