# This file isn't called in normal operation, just to update the TZ boundary DB.
# Should be called with `tz_world.*` files from http://efele.net/maps/tz/world/ in the working directory.
# Requires pyshp and shapely for py3k (from https://github.com/mwtoews/shapely/tree/py3)

import shapefile
from shapely.geometry import Polygon, mapping
import pymongo
from tapiriik.database import tzdb

print("Dropping boundaries collection")
tzdb.drop_collection("boundaries")

print("Setting up index")
tzdb.boundaries.ensure_index([("Boundary", pymongo.GEOSPHERE)])

print("Reading shapefile")
records = []
sf = shapefile.Reader("tz_world.shp")
shapeRecs = sf.shapeRecords()

ct = 0
total = len(shapeRecs)
for shape in shapeRecs:
	tzid = shape.record[0]
	print("%3d%% %s" % (round(ct * 100 / total), tzid))
	ct += 1
	polygon = Polygon(list(shape.shape.points))
	if not polygon.is_valid:
		polygon = polygon.buffer(0) # Resolves issues with most self-intersecting geometry
		assert polygon.is_valid
	record = {"TZID": tzid, "Boundary": mapping(polygon)}
	tzdb.boundaries.insert(record) # Would be bulk insert, but that makes it a pain to debug geometry issues
