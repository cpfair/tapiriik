from django.shortcuts import render, redirect

def supported_services_poll(req):
	return render(req, "supported-services-poll.html", {"voter_key": req.user["_id"] if req.user else ""}) # Should probably do something with ancestor accounts?