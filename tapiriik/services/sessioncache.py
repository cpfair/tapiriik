from datetime import datetime
from tapiriik.database import redis
import pickle

class SessionCache:
    def __init__(self, scope, lifetime, freshen_on_get=False):
        self._lifetime = lifetime
        self._autorefresh = freshen_on_get
        self._scope = scope
        self._cacheKey = "sessioncache:%s:%s" % (self._scope, "%s")

    def Get(self, pk, freshen=False):
        res = redis.get(self._cacheKey % pk)
        if res:
            try:
                res = pickle.loads(res)
            except pickle.UnpicklingError:
                redis.delete(self._cacheKey % pk)
                res = None
            else:
                if self._autorefresh or freshen:
                    redis.expire(self._cacheKey % pk, self._lifetime)
            return res

    def Set(self, pk, value, lifetime=None):
        lifetime = lifetime or self._lifetime
        redis.setex(self._cacheKey % pk, pickle.dumps(value), lifetime)
