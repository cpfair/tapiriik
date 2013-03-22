from tapiriik.auth import User
from tapiriik.services import Service
from django.shortcuts import render, redirect
from django import forms
from django.http import HttpResponse
import json


def config_save(req, service):
    if not req.user:
        return HttpResponse(status=403)

    conn = User.GetConnectionRecord(req.user, service)
    if not conn:
        return HttpResponse(status=404)
    Service.SetConfiguration(json.loads(req.POST["config"]), conn)
    return HttpResponse()


def config_flow_save(req, service):
    if not req.user:
        return HttpResponse(status=403)
    conns = User.GetConnectionRecordsByUser(req.user)
    if service not in [x["Service"] for x in conns]:
        return HttpResponse(status=404)
    sourceSvc = [x for x in conns if x["Service"] == service][0]
    #  the JS doesn't resolve the flow exceptions, it just passes in the expanded config flags for the edited service (which will override other flowexceptions)
    flowFlags = json.loads(req.POST["flowFlags"])
    for destSvc in [x for x in conns if x["Service"] != service]:
        User.SetFlowException(req.user, sourceSvc, destSvc, destSvc["Service"] in flowFlags["forward"], destSvc["Service"] in flowFlags["backward"])
    return HttpResponse()


class DropboxConfigForm(forms.Form):
    path = forms.CharField(label="Dropbox sync path")
    syncUntagged = forms.BooleanField(label="Sync untagged activities", required=False)


def dropbox(req):
    if not req.user:
        return HttpResponse(status=403)
    conn = User.GetConnectionRecord(req.user, "dropbox")
    if req.method == "POST":
        form = DropboxConfigForm(req.POST)
        if form.is_valid():
            Service.SetConfiguration({"SyncRoot": form.cleaned_data['path'], "UploadUntagged": form.cleaned_data['syncUntagged']}, conn)
            return redirect("dashboard")
    else:
        conf = Service.GetConfiguration(conn)
        form = DropboxConfigForm({"path": conf["SyncRoot"], "syncUntagged": conf["UploadUntagged"]})
    return render(req, "config/dropbox.html", {"form": form})
