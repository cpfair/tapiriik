from datetime import datetime, timedelta
from .interchange import WaypointType, ActivityStatisticUnit, ActivityType, LapIntensity, LapTriggerMethod
import struct
import sys
import pytz

class FITFileType:
	Activity = 4 # The only one we care about now.

class FITManufacturer:
	DEVELOPMENT = 255 # $1500/year for one of these numbers.

class FITEvent:
	Timer = 0
	Lap = 9
	Activity = 26

class FITEventType:
	Start = 0
	Stop = 1

# It's not a coincidence that these enums match the ones in interchange perfectly
class FITLapIntensity:
	Active = 0
	Rest = 1
	Warmup = 2
	Cooldown = 3

class FITLapTriggerMethod:
    Manual = 0
    Time = 1
    Distance = 2
    PositionStart = 3
    PositionLap = 4
    PositionWaypoint = 5
    PositionMarked = 6
    SessionEnd = 7
    FitnessEquipment = 8


class FITActivityType:
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
		self.InvalidValue = invalid

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
	def __init__(self):
		self._types = {}
		self._messageTemplates = {}
		self._definitions = {}
		self._result = []
		# All our convience functions for preparing the field types to be packed.
		def stringFormatter(input):
			raise Exception("Not implemented")
		def dateTimeFormatter(input):
			# UINT32
			# Seconds since UTC 00:00 Dec 31 1989. If <0x10000000 = system time
			if input is None:
				return struct.pack("<I", 0xFFFFFFFF)
			delta = round((input - datetime(hour=0, minute=0, month=12, day=31, year=1989)).total_seconds())
			return struct.pack("<I", delta)
		def msecFormatter(input):
			# UINT32
			if input is None:
				return struct.pack("<I", 0xFFFFFFFF)
			return struct.pack("<I", round((input if type(input) is not timedelta else input.total_seconds()) * 1000))
		def mmPerSecFormatter(input):
			# UINT16
			if input is None:
				return struct.pack("<H", 0xFFFF)
			return struct.pack("<H", round(input * 1000))
		def cmFormatter(input):
			# UINT32
			if input is None:
				return struct.pack("<I", 0xFFFFFFFF)
			return struct.pack("<I", round(input * 100))
		def altitudeFormatter(input):
			# UINT16
			if input is None:
				return struct.pack("<H", 0xFFFF)
			return struct.pack("<H", round((input + 500) * 5)) # Increments of 1/5, offset from -500m :S
		def semicirclesFormatter(input):
			# SINT32
			if input is None:
				return struct.pack("<i", 0x7FFFFFFF) # FIT-defined invalid value
			return struct.pack("<i", round(input * (2 ** 31 / 180)))


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
		defType("date_time", 0x86, 4, None, 0xFFFFFFFF, formatter=dateTimeFormatter)
		defType("duration_msec", 0x86, 4, None, 0xFFFFFFFF, formatter=msecFormatter)
		defType("distance_cm", 0x86, 4, None, 0xFFFFFFFF, formatter=cmFormatter)
		defType("mmPerSec", 0x84, 2, None, 0xFFFF, formatter=mmPerSecFormatter)
		defType("semicircles", 0x85, 4, None, 0x7FFFFFFF, formatter=semicirclesFormatter)
		defType("altitude", 0x84, 2, None, 0xFFFF, formatter=altitudeFormatter)

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
			59, "total_moving_time", "duration_msec",
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
			49, "avg_altitude", "altitude",
			50, "max_altitude", "altitude",
			71, "min_altitude", "altitude",
			57, "avg_temperature", "sint8",
			58, "max_temperature", "sint8")

		defMsg("lap", 19,
			253, "timestamp", "date_time",
			0, "event", "enum",
			1, "event_type", "enum",
			25, "sport", "enum",
			23, "intensity", "enum",
			24, "lap_trigger", "enum",
			2, "start_time", "date_time", # Vs timestamp, which was whenever the record was "written"/end of the session
			7, "total_elapsed_time", "duration_msec", # Including pauses
			8, "total_timer_time", "duration_msec", # Excluding pauses
			52, "total_moving_time", "duration_msec",
			9, "total_distance", "distance_cm",
			11,"total_calories", "uint16",
			13, "avg_speed", "mmPerSec",
			14, "max_speed", "mmPerSec",
			15, "avg_heart_rate", "uint8",
			16, "max_heart_rate", "uint8",
			17, "avg_cadence", "uint8", # FIT rolls run and bike cadence into one
			18, "max_cadence", "uint8",
			19, "avg_power", "uint16",
			20, "max_power", "uint16",
			21, "total_ascent", "uint16",
			22, "total_descent", "uint16",
			42, "avg_altitude", "altitude",
			43, "max_altitude", "altitude",
			62, "min_altitude", "altitude",
			50, "avg_temperature", "sint8",
			51, "max_temperature", "sint8"
			)

		defMsg("record", 20,
			253, "timestamp", "date_time",
			0, "position_lat", "semicircles",
			1, "position_long", "semicircles",
			2, "altitude", "altitude",
			3, "heart_rate", "uint8",
			4, "cadence", "uint8",
			5, "distance", "distance_cm",
			6, "speed", "mmPerSec",
			7, "power", "uint16",
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
		if set(field_names) - set(global_message.FieldNameList):
			raise ValueError("Attempting to use undefined fields %s" % (set(field_names) - set(global_message.FieldNameList)))
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
					if sanitized_value is None:
						result = struct.pack("<" + field_type.PackFormat, field_type.InvalidValue)
					else:
						if field_type.PackFormat in ["B","b", "H", "h", "I", "i"]:
							sanitized_value = round(sanitized_value)
						try:
							result = struct.pack("<" + field_type.PackFormat, sanitized_value)
						except struct.error as e: # I guess more specific exception types were too much to ask for.
							if "<=" in str(e) or "out of range" in str(e):
								result = struct.pack("<" + field_type.PackFormat, field_type.InvalidValue)
							else:
								raise
			except Exception as e:
				raise Exception("Failed packing %s=%s - %s" % (field_name, kwargs[field_name], e))
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
		# ActivityType.MountainBiking: 8 there's an issue with cadence upload and this type with GC, so...
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

	def Dump(act, supplant_timer_time_with_moving_time=False):
		def toUtc(ts):
			if ts.tzinfo:
				return ts.astimezone(pytz.utc).replace(tzinfo=None)
			else:
				raise ValueError("Need TZ data to produce FIT file")
		fmg = FITMessageGenerator()
		fmg.GenerateMessage("file_id", type=FITFileType.Activity, time_created=datetime.utcnow(), manufacturer=FITManufacturer.DEVELOPMENT, serial_number=1, product=15706)

		sport = FITIO._sportMap[act.Type] if act.Type in FITIO._sportMap else 0
		subSport = FITIO._subSportMap[act.Type] if act.Type in FITIO._subSportMap else 0

		session_stats = {
			"total_elapsed_time": act.EndTime - act.StartTime,
		}

		# FIT doesn't have different fields for this, but it does have a different interpretation - we eventually need to divide by two in the running case.
		# Further complicating the issue is that most sites don't differentiate the two, so they'll end up putting the run cadence back into the bike field.
		use_run_cadence = act.Type in [ActivityType.Running, ActivityType.Walking, ActivityType.Hiking]
		def _resolveRunCadence(bikeCad, runCad):
			nonlocal use_run_cadence
			if use_run_cadence:
				return runCad/2 if runCad is not None else (bikeCad/2 if bikeCad is not None else None)
			else:
				return bikeCad

		def _mapStat(dict, key, value):
			if value is not None:
				dict[key] = value

		_mapStat(session_stats, "total_moving_time", act.Stats.MovingTime.Value)
		_mapStat(session_stats, "total_timer_time", act.Stats.TimerTime.Value)
		if supplant_timer_time_with_moving_time: # This is a bug in the way Strava handles moving/timer time for Running-type activities
			_mapStat(session_stats, "total_timer_time", act.Stats.MovingTime.Value)
		_mapStat(session_stats, "total_distance", act.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
		_mapStat(session_stats, "total_calories", act.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value)
		_mapStat(session_stats, "avg_speed", act.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Average)
		_mapStat(session_stats, "max_speed", act.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Max)
		_mapStat(session_stats, "avg_heart_rate", act.Stats.HR.Average)
		_mapStat(session_stats, "max_heart_rate", act.Stats.HR.Max)
		_mapStat(session_stats, "avg_cadence", _resolveRunCadence(act.Stats.Cadence.Average, act.Stats.RunCadence.Average))
		_mapStat(session_stats, "max_cadence", _resolveRunCadence(act.Stats.Cadence.Max, act.Stats.RunCadence.Max))
		_mapStat(session_stats, "avg_power", act.Stats.Power.Average)
		_mapStat(session_stats, "max_power", act.Stats.Power.Max)
		_mapStat(session_stats, "total_ascent", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Gain)
		_mapStat(session_stats, "total_descent", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Loss)
		_mapStat(session_stats, "avg_altitude", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Average)
		_mapStat(session_stats, "max_altitude", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Max)
		_mapStat(session_stats, "min_altitude", act.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Min)
		_mapStat(session_stats, "avg_temperature", act.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Average)
		_mapStat(session_stats, "max_temperature", act.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Max)

		inPause = False
		for lap in act.Laps:
			for wp in lap.Waypoints:
				if wp.Type == WaypointType.Resume and inPause:
					fmg.GenerateMessage("event", timestamp=toUtc(wp.Timestamp), event=FITEvent.Timer, event_type=FITEventType.Start)
					inPause = False
				elif wp.Type == WaypointType.Pause and not inPause:
					fmg.GenerateMessage("event", timestamp=toUtc(wp.Timestamp), event=FITEvent.Timer, event_type=FITEventType.Stop)
					inPause = True

				rec_contents = {"timestamp": toUtc(wp.Timestamp)}
				if wp.Location:
					rec_contents.update({"position_lat": wp.Location.Latitude, "position_long": wp.Location.Longitude})
					if wp.Location.Altitude is not None:
						rec_contents.update({"altitude": wp.Location.Altitude})
				if wp.HR is not None:
					rec_contents.update({"heart_rate": wp.HR})
				if wp.RunCadence is not None:
					rec_contents.update({"cadence": wp.RunCadence})
				if wp.Cadence is not None:
					rec_contents.update({"cadence": wp.Cadence})
				if wp.Power is not None:
					rec_contents.update({"power": wp.Power})
				if wp.Temp is not None:
					rec_contents.update({"temperature": wp.Temp})
				if wp.Calories is not None:
					rec_contents.update({"calories": wp.Calories})
				if wp.Distance is not None:
					rec_contents.update({"distance": wp.Distance})
				if wp.Speed is not None:
					rec_contents.update({"speed": wp.Speed})
				fmg.GenerateMessage("record", **rec_contents)
			# Man, I love copy + paste and multi-cursor editing
			# But seriously, I'm betting that, some time down the road, a stat will pop up in X but not in Y, so I won't feel so bad about the C&P abuse
			lap_stats = {}
			_mapStat(lap_stats, "total_elapsed_time", lap.EndTime - lap.StartTime)
			_mapStat(lap_stats, "total_moving_time", lap.Stats.MovingTime.Value)
			_mapStat(lap_stats, "total_timer_time", lap.Stats.TimerTime.Value)
			if supplant_timer_time_with_moving_time:
				_mapStat(lap_stats, "total_timer_time", lap.Stats.MovingTime.Value)
			_mapStat(lap_stats, "total_distance", lap.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)
			_mapStat(lap_stats, "total_calories", lap.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value)
			_mapStat(lap_stats, "avg_speed", lap.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Average)
			_mapStat(lap_stats, "max_speed", lap.Stats.Speed.asUnits(ActivityStatisticUnit.MetersPerSecond).Max)
			_mapStat(lap_stats, "avg_heart_rate", lap.Stats.HR.Average)
			_mapStat(lap_stats, "max_heart_rate", lap.Stats.HR.Max)
			_mapStat(lap_stats, "avg_cadence", _resolveRunCadence(lap.Stats.Cadence.Average, lap.Stats.RunCadence.Average))
			_mapStat(lap_stats, "max_cadence", _resolveRunCadence(lap.Stats.Cadence.Max, lap.Stats.RunCadence.Max))
			_mapStat(lap_stats, "avg_power", lap.Stats.Power.Average)
			_mapStat(lap_stats, "max_power", lap.Stats.Power.Max)
			_mapStat(lap_stats, "total_ascent", lap.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Gain)
			_mapStat(lap_stats, "total_descent", lap.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Loss)
			_mapStat(lap_stats, "avg_altitude", lap.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Average)
			_mapStat(lap_stats, "max_altitude", lap.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Max)
			_mapStat(lap_stats, "min_altitude", lap.Stats.Elevation.asUnits(ActivityStatisticUnit.Meters).Min)
			_mapStat(lap_stats, "avg_temperature", lap.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Average)
			_mapStat(lap_stats, "max_temperature", lap.Stats.Temperature.asUnits(ActivityStatisticUnit.DegreesCelcius).Max)

			# These are some really... stupid lookups.
			# Oh well, futureproofing.
			lap_stats["intensity"] = ({
					LapIntensity.Active: FITLapIntensity.Active,
					LapIntensity.Rest: FITLapIntensity.Rest,
					LapIntensity.Warmup: FITLapIntensity.Warmup,
					LapIntensity.Cooldown: FITLapIntensity.Cooldown,
				})[lap.Intensity]
			lap_stats["lap_trigger"] = ({
					LapTriggerMethod.Manual: FITLapTriggerMethod.Manual,
					LapTriggerMethod.Time: FITLapTriggerMethod.Time,
					LapTriggerMethod.Distance: FITLapTriggerMethod.Distance,
					LapTriggerMethod.PositionStart: FITLapTriggerMethod.PositionStart,
					LapTriggerMethod.PositionLap: FITLapTriggerMethod.PositionLap,
					LapTriggerMethod.PositionWaypoint: FITLapTriggerMethod.PositionWaypoint,
					LapTriggerMethod.PositionMarked: FITLapTriggerMethod.PositionMarked,
					LapTriggerMethod.SessionEnd: FITLapTriggerMethod.SessionEnd,
					LapTriggerMethod.FitnessEquipment: FITLapTriggerMethod.FitnessEquipment,
				})[lap.Trigger]
			fmg.GenerateMessage("lap", timestamp=toUtc(lap.EndTime), start_time=toUtc(lap.StartTime), event=FITEvent.Lap, event_type=FITEventType.Start, sport=sport, **lap_stats)


		# These need to be at the end for Strava
		fmg.GenerateMessage("session", timestamp=toUtc(act.EndTime), start_time=toUtc(act.StartTime), sport=sport, sub_sport=subSport, event=FITEvent.Timer, event_type=FITEventType.Start, **session_stats)
		fmg.GenerateMessage("activity", timestamp=toUtc(act.EndTime), local_timestamp=act.EndTime.replace(tzinfo=None), num_sessions=1, type=FITActivityType.GENERIC, event=FITEvent.Activity, event_type=FITEventType.Stop)
		records = fmg.GetResult()
		header = FITIO._generateHeader(len(records))
		crc = FITIO._calculateCRC(records, FITIO._calculateCRC(header))
		return header + records + struct.pack("<H", crc)
