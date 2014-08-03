from tapiriik.services import Service
from tapiriik.services.ratelimiting import RateLimit

for svc in Service.List():
	RateLimit.Refresh(svc.ID, svc.GlobalRateLimits)
