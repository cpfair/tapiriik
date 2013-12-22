from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.sessioncache import SessionCache
from tapiriik.services.fit import FITIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import pytz
import re
import zlib
import os
import logging
import pickle

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "http://www.endomondo.com/profile/{0}"
    UserActivityURL = "http://www.endomondo.com/workouts/{1}/{0}"


    _activityMappings = {
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,  # the order of these matters since it picks the first match for uploads
        1:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        9:  ActivityType.Rowing,  # canoeing
        18: ActivityType.Walking,
        14: ActivityType.Walking,  # fitness walking
        16: ActivityType.Hiking,
        17: ActivityType.Hiking,  # orienteering
        20: ActivityType.Swimming,
        40: ActivityType.Swimming,  # scuba diving
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    _reverseActivityMappings = {  # so that ambiguous events get mapped back to reasonable types
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        18: ActivityType.Walking,
        16: ActivityType.Hiking,
        20: ActivityType.Swimming,
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways

    def WebInit(self):
        pass

    def RetrieveAuthorizationToken(self, req, level):
        pass

    def RevokeAuthorization(self, serviceRecord):
        pass

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        activities = []
        exclusions = []

        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):

        return activity

    def UploadActivity(self, serviceRecord, activity):
        pass

    def DeleteCachedData(self, serviceRecord):
        pass
