from datetime import datetime, timedelta
from .interchange import WaypointType, ActivityStatisticUnit, ActivityType
import struct
import sys
import pytz

class FITFileTypes:
	Activity = 4 # The only one we care about now.

class FITManufacturers:
	DEVELOPMENT = 255 # $1500/year for one of these numbers.

class FITEvents:
	Timer = 0
	Lap = 9
	Activity = 26

class FITEventTypes:
	Start = 0
	Stop = 1

class FITActivityTypes:
	GENERIC = 0
	RUNNING = 1
	CYCLING = 2
	TRANSITION = 3
	FITNESS_EQUIPMENT = 4
	SWIMMING = 5
	WALKING = 6
	ALL = 254

class FITMessageDataType:
	def __init__(self, name, typeField, size, packFormat, invalid, formatter=None):
		self.Name = name
		self.TypeField = typeField
		self.Size = size
		self.PackFormat = packFormat
		self.Formatter = formatter

class FITMessageTemplate:
	def __init__(self, name, number, *args, fields=None):
		self.Name = name
		self.Number = number
		self.Fields = {}
		self.FieldNameSet = set()
		self.FieldNameList = []
		if len(args) == 1 and type(args[0]) is dict:
			fields = args[0]
			self.Fields = fields
			self.FieldNameSet = set(fields.keys()) # It strikes me that keys might already be a set?
		else:
			# Supply fields in order NUM, NAME, TYPE
			for x in range(0, int(len(args)/3)):
				n = x * 3
				self.Fields[args[n+1]] = {"Name": args[n+1], "Number": args[n], "Type": args[n+2]}
				self.FieldNameSet.add(args[n+1])
		sortedFields = list(self.Fields.values())
		sortedFields.sort(key = lambda x: x["Number"])
		self.FieldNameList = [x["Name"] for x in sortedFields] # *ordered*


class FITMessageGenerator:
	_types = {}
	_messageTemplates = {}
	_definitions = {}
	_result = []
	def __init__(self):
		# All our convience functions for preparing the field types to be packed.
		def stringFormatter(input):
			raise Exception("Not implemented")
		def dateTimeFormatter(input):
			# UINT32
			# Seconds since UTC 00:00 Dec 31 1989. If <0x10000000 = system time
			delta = int((input - datetime(hour=0, minute=0, month=12, day=31, year=1989)).total_seconds())
			return struct.pack("<I", delta)
		def msecFormatter(input):
			# uint32
			return struct.pack("<I", int((input if type(input) is not timedelta else input.total_seconds()) * 1000))
		def mmPerSecFormatter(input):
			# UINT16
			return struct.pack("<H", int(input * 1000))
		def cmFormatter(input):
			# UINT32
			return struct.pack("<I", int(input * 100))
		def altitudeFormatter(input):
			# UINT16
			return struct.pack("<H", int((input + 500) / 5)) # Increments of 1/5, offset from -500m :S
		def semicirclesFormatter(input):
			# SINT32
			return struct.pack("<i", int(input * (2 ** 31 / 180)))


		def defType(name, *args, **kwargs):

			aliases = [name] if type(name) is not list else name
			# Cheap cheap cheap
			for alias in aliases:
				self._types[alias] = FITMessageDataType(alias, *args, **kwargs)

		defType(["enum", "file"], 0x00, 1, "B", 0xFF)
		defType("sint8", 0x01, 1, "b", 0x7F)
		defType("uint8", 0x02, 1, "B", 0xFF)
		defType("sint16", 0x83, 2, "h", 0x7FFF)
		defType(["uint16", "manufacturer"], 0x84, 2, "H", 0xFFFF)
		defType("sint32", 0x85, 4, "i", 0x7FFFFFFF)
		defType("uint32", 0x86, 4, "I", 0xFFFFFFFF)
		defType("string", 0x07, None, None, 0x0, formatter=stringFormatter)
		defType("float32", 0x88, 4, "f", 0xFFFFFFFF)
		defType("float64", 0x89, 8, "d", 0xFFFFFFFFFFFFFFFF)
		defType("uint8z", 0x0A, 1, "B", 0x00)
		defType("uint16z", 0x0B, 2, "H", 0x00)
		defType("uint32z", 0x0C, 4, "I", 0x00)
		defType("byte", 0x0D, 1, "B", 0xFF) # This isn't totally correct, docs say "an array of bytes"

		# Not strictly FIT fields, but convenient.
		defType("date_time", 0x86, 4, None, 0x0, formatter=dateTimeFormatter)
		defType("duration_msec", 0x86, 4, None, 0x0, formatter=msecFormatter)
		defType("distance_cm", 0x86, 4, None, 0x0, formatter=cmFormatter)
		defType("mmPerSec", 0x84, 2, None, 0x0, formatter=mmPerSecFormatter)
		defType("semicircles", 0x85, 4, None, 0x0, formatter=semicirclesFormatter)
		defType("altitude", 0x84, 2, None, 0x0, formatter=altitudeFormatter)

		def defMsg(name, *args):
			self._messageTemplates[name] = FITMessageTemplate(name, *args)

		defMsg("file_id", 0,
			0, "type", "file",
			1, "manufacturer", "manufacturer",
			2, "product", "uint16",
			3, "serial_number", "uint32z",
			4, "time_created", "date_time",
			5, "number", "uint16")

		defMsg("activity", 34,
			253, "timestamp", "date_time",
			1, "num_sessions", "uint16",
			2, "type", "enum",
			3, "event", "enum", # Required
			4, "event_type", "enum",
			5, "local_timestamp", "date_time")

		defMsg("session", 18,
			253, "timestamp", "date_time",
			2, "start_time", "date_time", # Vs timestamp, which was whenever the record was "written"/end of the session
			7, "total_elapsed_time", "duration_msec", # Including pauses
			8, "total_timer_time", "duration_msec", # Excluding pauses
			5, "sport", "enum",
			6, "sub_sport", "enum",
			0, "event", "enum",
			1, "event_type", "enum",
			9, "total_distance", "distance_cm",
			11,"total_calories", "uint16",
			14, "avg_speed", "mmPerSec",
			15, "max_speed", "mmPerSec",
			16, "avg_heart_rate", "uint8",
			17, "max_heart_rate", "uint8",
			18, "avg_cadence", "uint8",
			19, "max_cadence", "uint8",
			20, "avg_power", "uint16",
			21, "max_power", "uint16",
			22, "total_ascent", "uint16",
			23, "total_descent", "uint16",
			50, "max_altitude", "uint16",
			71, "min_altitude", "uint16",
			57, "avg_temperature", "sint8",
			58, "max_temperature", "sint8")

		defMsg("lap", 19,
			253, "timestamp", "date_time",
			0, "event", "enum",
			1, "event_type", "enum")

		defMsg("record", 20,
			253, "timestamp", "date_time",
			0, "position_lat", "semicircles",
			1, "position_long", "semicircles",
			2, "altitude", "altitude",
			3, "heart_rate", "uint8",
			4, "cadence", "uint8",
			7, "power", "uint8",
			13, "temperature", "sint8",
			33, "calories", "uint16",
			)

		defMsg("event", 21,
			253, "timestamp", "date_time",
			0, "event", "enum",
			1, "event_type", "enum")

	def _write(self, contents):
		self._result.append(contents)

	def GetResult(self):
		return b''.join(self._result)

	def _defineMessage(self, local_no, global_message, field_names):
		assert local_no < 16 and local_no >= 0
		messageHeader = 0b01000000
		messageHeader = messageHeader | local_no

		local_fields = {}

		arch = 0 # Little-endian
		global_no = global_message.Number

		field_count = len(field_names)
		pack_tuple = (messageHeader, 0, arch, global_no, field_count)
		for field_name in global_message.FieldNameList:
			if field_name in field_names:
				field = global_message.Fields[field_name]
				field_type = self._types[field["Type"]]
				pack_tuple += (field["Number"], field_type.Size, field_type.TypeField)
				local_fields[field_name] = field
		self._definitions[local_no] = FITMessageTemplate(global_message.Name, local_no, local_fields)
		self._write(struct.pack("<BBBHB" + ("BBB" * field_count), *pack_tuple))
		return self._definitions[local_no]


	def GenerateMessage(self, name, **kwargs):
		globalDefn = self._messageTemplates[name]

		# Create a subset of the global message's fields
		localFieldNamesSet = set()
		for fieldName in kwargs:
			localFieldNamesSet.add(fieldName)

		# I'll look at this later
		compressTS = False

		# Are these fields covered by an existing local message type?
		active_definition = None
		for defn_n in self._definitions:
			defn = self._definitions[defn_n]
			if defn.Name == name:
				if defn.FieldNameSet == localFieldNamesSet:
					active_definition = defn

		# If not, create a new local message type with these fields
		if not active_definition:
			active_definition_no = len(self._definitions)
			active_definition = self._defineMessage(active_definition_no, globalDefn, localFieldNamesSet)

		if compressTS and active_definition.Number > 3:
			raise Exception("Can't use compressed timestamp when local message number > 3")

		messageHeader = 0
		if compressTS:
			messageHeader = messageHeader | (1 << 7)
			tsOffsetVal = -1 # TODO
			messageHeader = messageHeader | (active_definition.Number << 4)
		else:
			messageHeader = messageHeader | active_definition.Number

		packResult = [struct.pack("<B", messageHeader)]
		for field_name in active_definition.FieldNameList:
			field = active_definition.Fields[field_name]
			field_type = self._types[field["Type"]]
			try:
				if field_type.Formatter:
					result = field_type.Formatter(kwargs[field_name])
				else:
					sanitized_value = kwargs[field_name]
					if field_type.PackFormat in ["B","b", "H", "h", "I", "i"]:
						sanitized_value = int(sanitized_value)
					result = struct.pack("<" + field_type.PackFormat, sanitized_value)
			except Exception as e:
				raise Exception("Failed packing %s - %s" % (field_name, e))
			packResult.append(result)
		self._write(b''.join(packResult))


class FITIO:

	_sportMap = {
		ActivityType.Other: 0,
		ActivityType.Running: 1,
		ActivityType.Cycling: 2,
		ActivityType.MountainBiking: 2,
		ActivityType.Elliptical: 4,
		ActivityType.Swimming: 5,
	}
	_subSportMap = {
		ActivityType.MountainBiking: 8
	}
	def _calculateCRC(bytestring, crc=0):
		crc_table = [0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401, 0xA001, 0x6C00, 0x7800, 0xB401, 0x5000, 0x9C01, 0x8801, 0x4400]
		for byte in bytestring:
			tmp = crc_table[crc & 0xF]
			crc = (crc >> 4) & 0x0FFF
			crc = crc ^ tmp ^ crc_table[byte & 0xF]

			tmp = crc_table[crc & 0xF]
			crc = (crc >> 4) & 0x0FFF
			crc = crc ^ tmp ^ crc_table[(byte >> 4) & 0xF]
		return crc

	def _generateHeader(dataLength):
		# We need to call this once the final records are assembled and their length is known, to avoid having to seek back
		header_len = 12
		protocolVer = 16 # The FIT SDK code provides these in a very rounabout fashion
		profileVer = 810
		tag = ".FIT"
		return struct.pack("<BBHI4s", header_len, protocolVer, profileVer, dataLength, tag.encode("ASCII"))

	def Parse(raw_file):
		raise Exception("Not implemented")

	def Dump(act):
		def toUtc(ts):
			if ts.tzinfo:
				return ts.astimezone(pytz.utc).replace(tzinfo=None)
			else:
				raise ValueError("Need TZ data to produce FIT file")
		fmg = FITMessageGenerator()
		fmg.GenerateMessage("file_id", type=FITFileTypes.Activity, time_created=datetime.utcnow(), manufacturer=FITManufacturers.DEVELOPMENT, serial_number=1, product=15706)
		fmg.GenerateMessage("activity", timestamp=toUtc(act.EndTime), local_timestamp=act.EndTime.replace(tzinfo=None), num_sessions=1, type=FITActivityTypes.GENERIC, event=FITEvents.Activity, event_type=FITEventTypes.Start)

		session_stats = {
			"total_elapsed_time": act.EndTime - act.StartTime,
		}

		def _mapStat(key, value):
			nonlocal session_stats
			if value is not None:
				session_stats[key] = value

		_mapStat("total_timer_time", act.Stats.MovingTime.Value)
		_mapStat("total_distance", act.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
		_mapStat("total_calories", act.Stats.Kilocalories.Value)
		_mapStat("avg_speed", act.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Average)
		_mapStat("max_speed", act.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Max)
		_mapStat("avg_heart_rate", act.Stats.HR.Average)
		_mapStat("max_heart_rate", act.Stats.HR.Max)
		_mapStat("avg_cadence", act.Stats.Cadence.Average)
		_mapStat("max_cadence", act.Stats.Cadence.Max)
		_mapStat("avg_power", act.Stats.Power.Average)
		_mapStat("max_power", act.Stats.Power.Max)
		_mapStat("total_ascent", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Gain)
		_mapStat("total_descent", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Loss)
		_mapStat("max_altitude", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Max)
		_mapStat("min_altitude", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Min)
		_mapStat("avg_temperature", act.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Average)
		_mapStat("max_temperature", act.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Max)


		sport = FITIO._sportMap[act.Type] if act.Type in FITIO._sportMap else 0
		subSport = FITIO._subSportMap[act.Type] if act.Type in FITIO._subSportMap else 0

		fmg.GenerateMessage("session", timestamp=toUtc(act.EndTime), start_time=toUtc(act.StartTime), sport=sport, sub_sport=subSport, event=FITEvents.Timer, event_type=FITEventTypes.Start, **session_stats)
		fmg.GenerateMessage("lap", timestamp=toUtc(act.StartTime), event=FITEvents.Lap, event_type=FITEventTypes.Start)

		inPause = False
		for wp in act.Waypoints:
			if wp.Type == WaypointType.Resume and inPause:
				fmg.GenerateMessage("event", timestamp=toUtc(wp.Timestamp), event=FITEvents.Timer, event_type=FITEventTypes.Start)
				inPause = False
			elif wp.Type == WaypointType.Pause and not inPause:
				fmg.GenerateMessage("event", timestamp=toUtc(wp.Timestamp), event=FITEvents.Timer, event_type=FITEventTypes.Stop)
				inPause = True
			elif wp.Type == WaypointType.Lap:
				fmg.GenerateMessage("lap", timestamp=toUtc(wp.Timestamp), event=FITEvents.Lap, event_type=FITEventTypes.Start)

			rec_contents = {"timestamp": toUtc(wp.Timestamp)}
			if wp.Location:
				rec_contents.update({"position_lat": wp.Location.Latitude, "position_long": wp.Location.Longitude})
				if wp.Location.Altitude is not None:
					rec_contents.update({"altitude": wp.Location.Altitude})
			if wp.HR is not None:
				rec_contents.update({"heart_rate": wp.HR})
			if wp.Cadence is not None:
				rec_contents.update({"cadence": wp.Cadence})
			if wp.Power is not None:
				rec_contents.update({"power": wp.Power})
			if wp.Temp is not None:
				rec_contents.update({"temperature": wp.Temp})
			if wp.Calories is not None:
				rec_contents.update({"calories": wp.Calories})
			fmg.GenerateMessage("record", **rec_contents)

		records = fmg.GetResult()
		header = FITIO._generateHeader(len(records))
		crc = FITIO._calculateCRC(records, FITIO._calculateCRC(header))
		return header + records + struct.pack("<H", crc)
