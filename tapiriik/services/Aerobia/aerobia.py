# Synchronisation module for aerobia.ru
# (c) 2018 Anton Ashmarin, aashmarin@gmail.com
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO
from tapiriik.services.sessioncache import SessionCache

import requests
import logging
import re
import os

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AerobiaService(ServiceBase):
    ID = "aerobia"
    DisplayName = "Aerobia"
    DisplayAbbreviation = "ARB"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True

    #common -> aerobia
    _activityMappings = {
        ActivityType.Running : 2,
        ActivityType.Cycling : 1,
        #ActivityType.MountainBiking,
        ActivityType.Walking : 19,
        #ActivityType.Hiking,
        #ActivityType.DownhillSkiing,
        ActivityType.CrossCountrySkiing : 3,
        #ActivityType.Skating,
        ActivityType.Swimming : 21,
        #ActivityType.Rowing,
        #ActivityType.Elliptical,
        #ActivityType.Gym,
        #ActivityType.Climbing,
        #ActivityType.StrengthTraining,
        ActivityType.Other : 68
    }

    #todo fill mappings
    _reverseActivityMappings = {
        1 : ActivityType.Cycling,
        2 : ActivityType.Running,

        68 : ActivityType.Other
    }

    SupportedActivities = list(_activityMappings.keys())

    SupportsHR = SupportsCadence = True

    SupportsActivityDeletion = True

    _sessionCache = SessionCache("aerobia", lifetime=timedelta(minutes=30), freshen_on_get=True)

    _urlRoot = "http://aerobia.ru/"
    _loginUrlRoot = _urlRoot + "users/sign_in"

    def _get_session(self, record=None, username=None, password=None, skip_cache=False):
        from tapiriik.auth.credential_storage import CredentialStore
        from tapiriik.requests_lib import patch_requests_user_agent

        # Without user-agent patch aerobia requests doesn't work
        patch_requests_user_agent('Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko')

        cached = self._sessionCache.Get(record.ExternalID if record else username)
        if cached and not skip_cache:
            logger.debug("Using cached credential")
            return cached
        if record:
            #  longing for C style overloads...
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            username = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])

        session = requests.Session()

        request_parameters = {"user[email]": username, "user[password]": password}
        res = session.post(self._loginUrlRoot, data=request_parameters)

        if res.status_code >= 500 and res.status_code < 600:
            raise APIException("Login exception %s - %s" % (res.status_code, res.text), user_exception=UserException(UserExceptionType.Authorization))

        # Userid is needed for urls
        id_match = re.search(r"users/(\d+)/workouts", res.text)
        if not id_match:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        
        session.user_id = id_match.group(1)
        
        # Token is passed with GET queries as a parameter
        token_match = re.search(r"meta content=\"(.+)\" name=\"csrf-token", res.text)
        if not token_match:
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        
        session.authenticity_token = token_match.group(1)

        self._sessionCache.Set(record.ExternalID if record else username, session)

        return session

    def _get_user_token(self, serviceRecord):
        userToken = None
        if serviceRecord:
            from tapiriik.auth.credential_storage import CredentialStore
            userToken = CredentialStore.Decrypt(serviceRecord.ExtendedAuthorization["UserToken"])
        return userToken

    def _with_auth(self, serviceRecord, params={}):
        # For whatever reason the authenticity_token needs to be a parameter
        params.update({"\"authenticity_token\"": self._get_user_token(serviceRecord)})
        return params

    def Authorize(self, username, password):
        from tapiriik.auth.credential_storage import CredentialStore
        session = self._get_session(username=username, password=password, skip_cache=True)

        secret = {
            "Email": CredentialStore.Encrypt(username), 
            "Password": CredentialStore.Encrypt(password)#, 
            #"UserToken": CredentialStore.Encrypt(session.authenticity_token)
            }
        return (session.user_id, {}, secret)

    def DownloadActivityList(self, serviceRecord, exhaustive_start_date=None):
        session = self._get_session(serviceRecord)

        activities = []
        exclusions = []
        #todo get date from the first request to get guaranteed last training
        date = datetime.now()
        date_format = "%Y-%m-01"

        while True:
            list_params = {"month": date.strftime(date_format)}
            dairy_data = session.get(self._urlRoot + "users/%s/workouts" %serviceRecord.ExternalID, params=list_params)

            act_data_list = self._extract_activities(dairy_data.text)
            for act_data in act_data_list:
                activity = self._create_activity(act_data)
                #todo need to exclude repeated data due to calendar output overlap 
                activities.append(activity)
            #todo will stop if user didn't do excercises during a particular month
            if len(act_data_list) == 0 or not exhaustive_start_date:
                break

            date = date - timedelta(days=1)
            date = date.replace(days=1)

        return activities, exclusions

    def _extract_activities(self, html):
        #todo parse html to get array of json training objects
        return []

    def _create_activity(self, activity_data):
        activity = UploadedActivity()
        #todo fill fields from activity_data
        
        activity.CalculateUID()
        return activity

    def DownloadActivity(self, serviceRecord, activity):
        session = self._get_session(serviceRecord)
        tcx_data = session.get(self._urlRoot + "export/workouts/%d/tcx" %activity.ServiceData["ActivityID"])
        #todo get notes!
        return TCXIO.Parse(tcx_data, activity)

    def UploadActivity(self, serviceRecord, activity):
        session = self._get_session(serviceRecord, skip_cache=True)
        tcx_data = TCXIO.Dump(activity)
        file = {"workout_file[file][]": (tcx_data, "tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".tcx")}
        #todo why session dont contains token?
        res = session.post(self._urlRoot + "import/files", params=self._with_auth(serviceRecord), files=file)
        res_obj = res.json()
        #confirm file upload
        session.get(res_obj.continue_path)
        #return just uploaded activity id
        #todo change training notes!
        return res_obj.id

    def DeleteActivity(self, serviceRecord, uploadId):
        session = self._get_session(serviceRecord)

        delete_parameters = {"_method" : "delete"}
        delete_parameters = self._with_auth(session, delete_parameters)
        session.post(self._urlRoot + "workouts/%d" %uploadId, data=delete_parameters)

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass
