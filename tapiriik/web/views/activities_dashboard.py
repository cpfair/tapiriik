from django.shortcuts import render
from django.http import HttpResponse
from tapiriik.database import db
from tapiriik.settings import WITHDRAWN_SERVICES
import json
import datetime

def activities_dashboard(req):
	return render(req, "activities-dashboard.html")

def activities_fetch_json(req):
	activityRecords = db.activity_records.find_one({"UserID": req.user["_id"]})
	if not activityRecords:
		return HttpResponse("[]", content_type="application/json")
	cleanedRecords = []
	for activity in activityRecords["Activities"]:
		# Strip down the record since most of this info isn't displayed
		for presence in activity["Prescence"]:
			del activity["Prescence"][presence]["Exception"]
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