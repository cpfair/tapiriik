from tapiriik.database import tzdb
from bson.son import SON

def TZLookup(lat, lng):
	pt = [lng, lat]
	res = tzdb.boundaries.find_one({"Boundary": {"$geoIntersects": {"$geometry": {"type":"Point", "coordinates": pt}}}}, {"TZID": True})
	if not res:
		res = tzdb.boundaries.find_one({"Boundary": SON([("$near", {"$geometry": {"type": "Point", "coordinates": pt}}), ("$maxDistance", 200000)])}, {"TZID": True})
	res = res["TZID"] if res else None
	if not res or res == "uninhabited":
		res = round(lng / 15)
	return res
