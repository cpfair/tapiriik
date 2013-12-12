from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.utils.http import urlencode
@csrf_exempt
def trainingpeaks_premium(request):
	ctx = {}
	if "password" in request.POST:
		ctx = {"password": request.POST["password"], "username": request.POST["username"], "personId": request.POST["personId"]}

	return render(request, "trainingpeaks_premium.html", ctx)