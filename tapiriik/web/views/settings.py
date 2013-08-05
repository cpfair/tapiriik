from django.shortcuts import render, redirect
from tapiriik.auth import User

def settings(request):
    available_settings = {
        "allow_activity_flow_exception_bypass_via_self":
            {"Title": "Route activities via",
            "Description": "Allows activities to flow through this service to avoid a flow exception that would otherwise prevent them arriving at a destination.",
            "Field": "checkbox"
            },
        "sync_private":
            {"Title": "Sync private activities",
            "Description": "By default, all activities will be synced. Unsetting this will prevent private activities being taken from this service.",
            "Field": "checkbox",
            "Available": ["strava", "runkeeper"]
            }
    }
    conns = User.GetConnectionRecordsByUser(request.user)

    for key, setting in available_settings.items():
        available_settings[key]["Values"] = {}

    for conn in conns:
        config = conn.GetConfiguration()
        #import pdb; pdb.set_trace()
        for key, setting in available_settings.items():
            if request.method == "POST":
                formkey = key + "_" + conn.Service.ID
                if setting["Field"] == "checkbox":
                    config[key] = formkey in request.POST
            available_settings[key]["Values"][conn.Service.ID] = config[key]

        if request.method == "POST":
            conn.SetConfiguration(config)
    if request.method == "POST":
        return redirect("settings_panel")

    return render(request, "settings.html", {"user": request.user, "settings": available_settings})
