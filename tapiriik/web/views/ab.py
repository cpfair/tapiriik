from tapiriik.database import db
from django.http import HttpResponse
from django.views.decorators.http import require_POST
import zlib
from datetime import datetime


_experiments = {}


def ab_register_experiment(key, variants):
	_experiments[key] = {"Variants": variants}

def ab_select_variant(key, userKey):
	selector = 0
	selector = zlib.adler32(bytes(str(key), "UTF-8"), selector)
	selector = zlib.adler32(bytes(str(userKey), "UTF-8"), selector)
	selector = selector % len(_experiments[key]["Variants"])
	return _experiments[key]["Variants"][selector]

def ab_experiment_begin(key, userKey):
	db.ab_experiments.insert({"User": userKey, "Experiment": key, "Begin": datetime.utcnow(), "Variant": ab_select_variant(key, userKey)})

def ab_user_experiment_begin(key, request):
	ab_experiment_begin(key, request.user["_id"])

def ab_experiment_complete(key, userKey, result):
	active_experiment = db.ab_experiments.find({"User": userKey, "Experiment": key, "Result": {"$exists": False}}, {"_id": 1}).sort("Begin", -1).limit(1)[0]
	db.ab_experiments.update({"_id": active_experiment["_id"]}, {"$set": {"Result": result}})

def ab_user_experiment_complete(key, request, result):
	ab_experiment_complete(key, request.user["_id"], result)

@require_POST
def ab_web_experiment_begin(request, key):
	if not request.user:
		return HttpResponse(status=403)
	if key not in _experiments:
		return HttpResponse(status=404)
	ab_user_experiment_begin(key, request)
	return HttpResponse()

def ab_experiment_context(request):
	context = {}
	if request.user:
		for key in _experiments.keys():
			context["ab_%s_%s" % (key, ab_select_variant(key, request.user["_id"]))] = True
	return context
