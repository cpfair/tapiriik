from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.fit import FITIO
from tapiriik.services.pwx import PWXIO
from lxml import etree

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import dateutil.parser
import requests
import time
import json
import os
import logging

logger = logging.getLogger(__name__)

class VeloHeroService(ServiceBase):
    ID = "velohero"
    DisplayName = "Velo Hero"
    DisplayAbbreviation = "VH"
    _urlRoot = "http://app.velohero.com"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesStationaryActivities = False
    
    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True
    
    SupportedActivities = ActivityType.List() # All.
    
    # http://app.velohero.com/sports/list?view=json
    _reverseActivityMappings={
        1:  ActivityType.Cycling,
        2:  ActivityType.Running,
        3:  ActivityType.Swimming,
        4:  ActivityType.Gym,
        5:  ActivityType.Other, # Strength
        6:  ActivityType.MountainBiking,
        7:  ActivityType.Hiking,
        8:  ActivityType.CrossCountrySkiing,
        # Currently not in use. Reserved for future use.
        9:  ActivityType.Other,
        10: ActivityType.Other,
        11: ActivityType.Other,
        12: ActivityType.Other,
        13: ActivityType.Other,
        14: ActivityType.Other,
        15: ActivityType.Other,
    }


    def _add_auth_params(self, params=None, record=None):
        """
        Adds username and password to the passed-in params,
        returns modified params dict.
        """
        
        from tapiriik.auth.credential_storage import CredentialStore
        
        if params is None:
            params = {}
        if record:
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            params['user'] = email
            params['pass'] = password
        return params


    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})


    def Authorize(self, email, password):
        """
        POST Username and Password
        
        URL: http://app.velohero.com/sso
        Parameters:
        user = username
        pass = password
        view = json
        
        The login was successful if you get HTTP status code 200.
        For other HTTP status codes, the login was not successful.
        """
        
        from tapiriik.auth.credential_storage import CredentialStore
        
        res = requests.post(self._urlRoot + "/sso",
                           params={'user': email, 'pass': password, 'view': 'json'})
        
        if res.status_code != 200:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        
        res.raise_for_status()
        res = res.json()
        if res["session"] is None:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        member_id = res["user-id"]
        if not member_id:
            raise APIException("Unable to retrieve user id", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        return (member_id, {}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})


    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...


    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...


    def _parseDateTime(self, date):
        return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")


    def _durationToSeconds(self, dur):
        parts = dur.split(":")
        return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])


    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        """
        GET List of Activities as JSON File
        
        URL: http://app.velohero.com/export/workouts/json
        Parameters:
        user      = username
        pass      = password
        date_from = YYYY-MM-DD
        date_to   = YYYY-MM-DD
        """
        activities           = []
        exclusions           = []
        discoveredWorkoutIds = []
        
        params = self._add_auth_params({}, record=serviceRecord)
        
        limitDateFormat = "%Y-%m-%d"
        
        if exhaustive:
            listEnd   = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = datetime(day=1, month=1, year=1980) # The beginning of time
        else:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = listEnd - timedelta(days=20) # Doesn't really matter
        
        params.update({"date_from": listStart.strftime(limitDateFormat), "date_to": listEnd.strftime(limitDateFormat)})
        logger.debug("Requesting %s to %s" % (listStart, listEnd))
        res = requests.get(self._urlRoot + "/export/workouts/json", params=params)
        
        if res.status_code != 200:
          if res.status_code == 403:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to retrieve activity list")
        
        res.raise_for_status()
        try:
            res = res.json()
        except ValueError:
            raise APIException("Could not decode activity list")
        if "workouts" not in res:
            raise APIException("No activities")
        for workout in res["workouts"]:
            workoutId = int(workout["id"])
            if workoutId in discoveredWorkoutIds:
               continue # There's the possibility of query overlap
            discoveredWorkoutIds.append(workoutId)
            if workout["file"] is not "1":
               logger.debug("Skip workout with ID: " + str(workoutId) + " (no file)")
               continue # Skip activity without samples (no PWX export)
            
            activity = UploadedActivity()
            
            logger.debug("Workout ID: " + str(workoutId))
            # Duration (dur_time)
            duration = self._durationToSeconds(workout["dur_time"])
            activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=duration)
            # Start time (date_ymd, start_time)
            startTimeStr = workout["date_ymd"] + " " + workout["start_time"]
            activity.StartTime = self._parseDateTime(startTimeStr)
            # End time (date_ymd, start_time) + dur_time
            activity.EndTime = self._parseDateTime(startTimeStr) + timedelta(seconds=duration)
            # Sport (sport_id)
            if workout["sport_id"] is not "0":
               activity.Type = self._reverseActivityMappings[int(workout["sport_id"])]
            else:
               activity.Type = ActivityType.Cycling
            # Distance (dist_km)
            activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(workout["dist_km"]))
            # Workout is hidden
            if workout["hide"] is "1":
               activity.Private
            
            activity.ServiceData = {"workoutId": workoutId}
            activity.CalculateUID()
            activities.append(activity)
        
        return activities, exclusions


    def DownloadActivity(self, serviceRecord, activity):
        """
        GET Activity as a PWX File
        
        URL: http://app.velohero.com/export/activity/pwx/<WORKOUT-ID>
        Parameters:
        user = username
        pass = password
        
        PWX export with laps.
        """
        
        workoutId = activity.ServiceData["workoutId"]
        logger.debug("Download PWX export with ID: " + str(workoutId))
        params = self._add_auth_params({}, record=serviceRecord)
        res = requests.get(self._urlRoot + "/export/activity/pwx/{}".format(workoutId), params=params)
        
        if res.status_code != 200:
          if res.status_code == 403:
            raise APIException("No authorization to download activity with workout ID: {}".format(workoutId), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to download activity with workout ID: {}".format(workoutId))
        
        activity = PWXIO.Parse(res.content, activity)
        
        activity.GPS = False
        flat_wps = activity.GetFlatWaypoints()
        for wp in flat_wps:
            if wp.Location and wp.Location.Latitude and wp.Location.Longitude:
                activity.GPS = True
                break
        
        return activity


    def UploadActivity(self, serviceRecord, activity):
        """
        POST a Multipart-Encoded File
        
        URL: http://app.velohero.com/upload/file
        Parameters:
        user = username
        pass = password
        view = json
        file = multipart-encodes file (fit, tcx, pwx, gpx, srm, hrm...)
        
        Maximum file size per file is 16 MB.
        """
        
        fit_file = FITIO.Dump(activity)
        files = {"file": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", fit_file)}
        params = self._add_auth_params({"view":"json"}, record=serviceRecord)
        res = requests.post(self._urlRoot + "/upload/file", files=files, params=params)
        
        if res.status_code != 200:
          if res.status_code == 403:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
          raise APIException("Unable to upload activity")
        
        res.raise_for_status()
        res = res.json()
        if "error" in res:
            raise APIException(res["error"])


