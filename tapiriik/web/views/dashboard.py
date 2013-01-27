from django.http import HttpResponse
from django.shortcuts import render

def dashboard(req):
    if req.user is not None:
        req.user["ConnectedServiceIDs"]=[x["Service"] for x in req.user["ConnectedServices"]] if "ConnectedServices" in req.user else []
    return render(req,"dashboard.html",{"user": req.user})
