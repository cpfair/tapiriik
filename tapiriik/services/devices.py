import re

class DeviceIdentifierType:
	FIT = "fit"
	TCX = "tcx"
	GC = "gc"

class DeviceIdentifier:
	def Match(self, query):
		compareDict = dict(self.__dict__)
		compareDict.update(query)
		return compareDict == self.__dict__ # At the time it felt like a better idea than iterating through keys?

class FITDeviceIdentifier(DeviceIdentifier):
	def __init__(self, manufacturer, product=None):
		self.Type = DeviceIdentifierType.FIT
		self.Manufacturer = manufacturer
		self.Product = product

class TCXDeviceIdentifier(DeviceIdentifier):
	def __init__(self, name, productId=None):
		self.Type = DeviceIdentifierType.TCX
		self.Name = name
		self.ProductID = productId

class GCDeviceIdentifier(DeviceIdentifier):
	def __init__(self, name):
		# Edge 810 -> edge810
		# They're quite stubborn with giving the whole list of these device keys.
		# So this is really a guess.
		self.Key = re.sub("[^a-z0-9]", "", name.lower())
		self.Type = DeviceIdentifierType.GC

	def Match(self, query):
		# Add some fuzziness becaise I can't be bothered figuring out what the pattern is
		return query["Key"] == self.Key or query["Key"] == ("garmin%s" % self.Key)


class DeviceIdentifier:
	_identifierGroups = []

	def AddIdentifierGroup(*identifiers):
		DeviceIdentifier._identifierGroups.append(identifiers)

	def FindMatchingIdentifierOfType(type, query):
		for group in DeviceIdentifier._identifierGroups:
			for identifier in group:
				if identifier.Type != type:
					continue
				if identifier.Match(query):
					return identifier

	def FindEquivalentIdentifierOfType(type, identifier):
		if not identifier:
			return
		if identifier.Type == type:
			return identifier # We preemptively do this, so international variants have a chance of being preserved
		for group in DeviceIdentifier._identifierGroups:
			if identifier not in group:
				continue
			for altIdentifier in group:
				if altIdentifier.Type == type:
					return altIdentifier

class Device:
	def __init__(self, identifier, serial=None, verMaj=None, verMin=None):
		self.Identifier = identifier
		self.Serial = serial
		self.VersionMajor = verMaj
		self.VersionMinor = verMin


# I think Garmin devices' TCX ProductID match their FIT garmin_product id
# And, since the FIT SDK is lagging behind:
#  - Forerunner 620 is 1623

def _garminIdentifier(name, *fitIds):
	return [TCXDeviceIdentifier("Garmin %s" % name, fitIds[0]), GCDeviceIdentifier(name)] + [FITDeviceIdentifier(1, fitId) for fitId in fitIds]

# This list is REGEXed from the FIT SDK - I have no clue what some of the entries are...
# Some products have international variants with different FIT IDs - the first ID given is used for TCX
# DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("HRM1", 1)) - Garmin Connect reports itself as ID 1 too.
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("AXH01", 2))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("AXB01", 3))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("AXB02", 4))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("HRM2SS", 5))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("DSI_ALF02", 6))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 301", 473, 474, 475, 494))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 405", 717, 987))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 50", 782))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 60", 988))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("DSI_ALF01", 1011))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 310XT", 1018, 1446))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 500", 1036, 1199, 1213, 1387, 1422))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 110", 1124, 1274))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 800", 1169, 1333, 1334, 1497, 1386))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Chirp", 1253))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 200", 1325, 1555))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 910XT", 1328, 1537, 1600, 1664))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 920XT", 1765)) # The SDK isn't updated yet, don't have international variants
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("ALF04", 1341))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 610", 1345, 1410))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 210", 1360)) # In the SDK this is marked as "JAPAN" :S
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 70", 1436))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("AMX", 1461))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 10", 1482, 1688))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Swim", 1499))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Fenix", 1551))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Fenix 2", 1967))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Fenix 3", 2050))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 510", 1561, 1742, 1821))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 810", 1567, 1721, 1822, 1823))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge 1000", 1836))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Tempe", 1570))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("VIRB Elite", 1735)) # Where's the VIRB Proletariat?
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Edge Touring", 1736))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("HRM Run", 1752))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("SDM4", 10007))
DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Training Center", 20119))

DeviceIdentifier.AddIdentifierGroup(*_garminIdentifier("Forerunner 620", 1623))

# TomTom MySports Connect appears to produce these IDs for all of their
# models of GPS watches (Runner, MultiSport, and Cardio versions of the same).
DeviceIdentifier.AddIdentifierGroup(TCXDeviceIdentifier("TomTom GPS Sport Watch", 0), FITDeviceIdentifier(71, 0))
