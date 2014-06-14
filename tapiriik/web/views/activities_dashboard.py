from django.shortcuts import render, redirect
from django.http import HttpResponse
from tapiriik.auth import User
from tapiriik.database import db
from tapiriik.settings import WITHDRAWN_SERVICES
import json
import datetime

def activities_dashboard(req):
    if not req.user:
        return redirect("/")
    return render(req, "activities-dashboard.html")

def activities_fetch_json(req):
    if not req.user:
        return HttpResponse(status=403)

    retrieve_fields = [
        "Activities.Prescence",
        "Activities.Abscence",
        "Activities.Type",
        "Activities.Name",
        "Activities.StartTime",
        "Activities.EndTime",
        "Activities.Private",
        "Activities.Stationary"
    ]
    activityRecords = db.activity_records.find_one({"UserID": req.user["_id"]}, dict([(x, 1) for x in retrieve_fields]))
    conns = User.GetConnectionRecordsByUser(req.user)
    connectedServiceIds = [x.Service.ID for x in conns]

    if not activityRecords:
        return HttpResponse("[]", content_type="application/json")
    cleanedRecords = []
    for activity in activityRecords["Activities"]:
        # Strip down the record since most of this info isn't displayed
        for serviceId in activity["Prescence"]:
            del activity["Prescence"][serviceId]["Exception"]
            # Add service urls
            if ("ServiceKeys" in activity["Prescence"][serviceId] and
                activity["Prescence"][serviceId]["ServiceKeys"]):
                # show only first link
                serviceKey = activity["Prescence"][serviceId]["ServiceKeys"][0]
                if serviceId not in connectedServiceIds:
                    # Maybe they unlinked their account...
                    continue
                sourceSvcRecord = [x for x in conns if x.Service.ID == serviceId][0]
                svc = sourceSvcRecord.Service
                activity["Prescence"][serviceId]["url"] = svc.GenerateUserActivityURL(sourceSvcRecord, serviceKey)


        for abscence in activity["Abscence"]:
            if activity["Abscence"][abscence]["Exception"]:
                del activity["Abscence"][abscence]["Exception"]["InterventionRequired"]
                del activity["Abscence"][abscence]["Exception"]["ClearGroup"]
        # Don't really need these seperate at this point
        activity["Prescence"].update(activity["Abscence"])
        for svc in WITHDRAWN_SERVICES:
            if svc in activity["Prescence"]:
                del activity["Prescence"][svc]
        del activity["Abscence"]
        cleanedRecords.append(activity)


    dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime)  or isinstance(obj, datetime.date) else None

    return HttpResponse(json.dumps(cleanedRecords, default=dthandler), content_type="application/json")