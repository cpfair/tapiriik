from datetime import datetime

class SessionCache:
	def __init__(self, lifetime, freshen_on_get=False):
		self._lifetime = lifetime
		self._autorefresh = freshen_on_get
		self._cache = {}

	def Get(self, pk, freshen=False):
		if pk not in self._cache:
			return
		record = self._cache[pk]
		if record.Expired():
			del self._cache[pk]
			return None
		if self._autorefresh or freshen:
			record.Refresh()
		return record.Get()

	def Set(self, pk, value):
		self._cache[pk] = SessionCacheRecord(value, self._lifetime)

class SessionCacheRecord:
	def __init__(self, data, lifetime):
		self._value = data
		self._lifetime = lifetime
		self.Refresh()

	def Expired(self):
		return self._timestamp < datetime.utcnow() - self._lifetime

	def Refresh(self):
		self._timestamp = datetime.utcnow()

	def Get(self):
		return self._value
